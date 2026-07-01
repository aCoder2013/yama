package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import com.song.yama.example.raft.support.FaultInjectionRegistry;
import com.song.yama.example.raft.support.RaftKvClusterHarness;
import com.song.yama.example.raft.support.RaftKvHttpClient;
import org.junit.After;
import org.junit.Test;

/**
 * Integration fault tests using real Spring Boot nodes and HTTP APIs.
 */
public class RaftKvFaultIntegrationTest {

    private RaftKvClusterHarness cluster;
    private final RaftKvHttpClient client = new RaftKvHttpClient();

    @After
    public void cleanup() {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    @Test
    public void threeNodeHttpReplication() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        cluster.putOnLeader(client, "name", "alice");
    }

    @Test
    public void writeOnLeaderReadableOnFollowers() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderId = cluster.leaderId();
        cluster.putOnLeader(client, "city", "beijing");

        for (int id = 1; id <= 3; id++) {
            if (id != leaderId) {
                assertEquals("beijing", client.get(cluster.node(id).getPort(), "city"));
            }
        }
    }

    @Test
    public void leaderCrashFailoverAndContinueWrite() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int oldLeaderId = cluster.leaderId();
        cluster.putOnLeader(client, "before", "crash");

        cluster.stopNode(oldLeaderId);
        Thread.sleep(3000);
        cluster.waitForLeader(80);

        int newLeaderId = cluster.leaderId();
        assertNotEquals(oldLeaderId, newLeaderId);
        client.put(cluster.leaderPort(), "after", "failover");
        cluster.assertKvConsistent(client, "after", "failover");

        for (int id = 1; id <= 3; id++) {
            if (id != oldLeaderId) {
                assertEquals("crash", client.get(cluster.node(id).getPort(), "before"));
            }
        }
    }

    @Test
    public void crashedFollowerCatchesUpOnRestart() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int followerId = 1;
        while (followerId == cluster.leaderId()) {
            followerId++;
        }

        cluster.stopNode(followerId);
        client.put(cluster.leaderPort(), "while-down", "value");
        cluster.assertKvConsistent(client, "while-down", "value");

        cluster.restartNode(followerId);
        client.waitFor(cluster.node(followerId).getPort(), "while-down", "value", 80);
    }

    @Test
    public void networkPartitionMakesFollowerStaleUntilHeal() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderId = cluster.leaderId();
        cluster.putOnLeader(client, "synced", "yes");

        int isolatedId = 1;
        while (isolatedId == leaderId) {
            isolatedId++;
        }

        FaultInjectionRegistry.isolate(isolatedId);
        client.put(cluster.leaderPort(), "newkey", "newval");
        Thread.sleep(1500);

        assertEquals("newval", client.get(cluster.leaderPort(), "newkey"));
        assertNull(client.get(cluster.node(isolatedId).getPort(), "newkey"));
        assertEquals("yes", client.get(cluster.node(isolatedId).getPort(), "synced"));

        FaultInjectionRegistry.recover();
        Thread.sleep(2000);
        client.waitFor(cluster.node(isolatedId).getPort(), "newkey", "newval", 80);
    }

    @Test
    public void isolatedOldLeaderMajorityReElects() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int oldLeaderId = cluster.leaderId();
        cluster.putOnLeader(client, "seed", "data");

        FaultInjectionRegistry.isolate(oldLeaderId);
        Thread.sleep(3000);

        assertEquals(1, cluster.countLeadersExcluding(oldLeaderId));
        int activeLeaderId = leaderIdExcluding(oldLeaderId);
        assertNotEquals(oldLeaderId, activeLeaderId);

        client.put(cluster.node(activeLeaderId).getPort(), "majority", "write");
        for (int id = 1; id <= 3; id++) {
            if (id != oldLeaderId) {
                client.waitFor(cluster.node(id).getPort(), "majority", "write", 100);
            }
        }

        FaultInjectionRegistry.recover();
        Thread.sleep(2000);
        cluster.waitForLeader(80);
        assertEquals(1, countLeaders());
    }

    private int leaderIdExcluding(int excludedId) {
        for (int id = 1; id <= 3; id++) {
            if (id != excludedId && cluster.node(id).isRunning() && cluster.node(id).isLeader()) {
                return id;
            }
        }
        return -1;
    }

    @Test
    public void manyWritesConsistentAcrossThreeNodes() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderPort = cluster.leaderPort();
        for (int i = 0; i < 12; i++) {
            client.put(leaderPort, "k" + i, "v" + i);
        }
        Thread.sleep(2000);
        for (int i = 0; i < 12; i++) {
            cluster.assertKvConsistent(client, "k" + i, "v" + i);
        }
    }

    private int countLeaders() {
        int count = 0;
        for (int id = 1; id <= 3; id++) {
            if (cluster.node(id).isRunning() && cluster.node(id).isLeader()) {
                count++;
            }
        }
        return count;
    }
}
