package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import com.song.yama.example.raft.support.FaultInjectionRegistry;
import com.song.yama.example.raft.support.RaftKvClusterHarness;
import com.song.yama.example.raft.support.RaftKvHttpClient;
import org.junit.After;
import org.junit.Test;
import org.springframework.web.client.HttpServerErrorException;

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
        assertReadUnavailable(cluster.node(isolatedId).getPort(), "newkey");
        assertReadUnavailable(cluster.node(isolatedId).getPort(), "synced");

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

    @Test
    public void readIndexOnFollowerMatchesLeader() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderId = cluster.leaderId();
        cluster.putOnLeader(client, "color", "blue");

        for (int id = 1; id <= 3; id++) {
            if (id != leaderId) {
                assertEquals("blue", client.get(cluster.node(id).getPort(), "color"));
            }
        }
    }

    @Test
    public void readIndexAfterLeaderFailover() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int oldLeaderId = cluster.leaderId();
        cluster.putOnLeader(client, "persist", "value");

        cluster.stopNode(oldLeaderId);
        Thread.sleep(3000);
        cluster.waitForLeader(80);

        int newLeaderPort = cluster.leaderPort();
        assertEquals("value", client.get(newLeaderPort, "persist"));

        for (int id = 1; id <= 3; id++) {
            if (id != oldLeaderId) {
                assertEquals("value", client.get(cluster.node(id).getPort(), "persist"));
            }
        }
    }

    @Test
    public void isolatedOldLeaderReturns503OnRead() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int oldLeaderId = cluster.leaderId();
        cluster.putOnLeader(client, "seed", "data");

        FaultInjectionRegistry.isolate(oldLeaderId);
        Thread.sleep(3000);
        cluster.waitForLeader(80);

        assertReadUnavailable(cluster.node(oldLeaderId).getPort(), "seed");
        assertReadUnavailable(cluster.node(oldLeaderId).getPort(), "missing");
    }

    @Test
    public void partitionHealRestoresLinearizableRead() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderId = cluster.leaderId();
        cluster.putOnLeader(client, "synced", "yes");

        int isolatedId = 1;
        while (isolatedId == leaderId) {
            isolatedId++;
        }

        FaultInjectionRegistry.isolate(isolatedId);
        client.put(cluster.leaderPort(), "heal-key", "heal-val");
        Thread.sleep(1500);

        client.waitForReadUnavailable(cluster.node(isolatedId).getPort(), "heal-key", 30);

        FaultInjectionRegistry.recover();
        Thread.sleep(2000);
        client.waitFor(cluster.node(isolatedId).getPort(), "heal-key", "heal-val", 80);
        assertEquals("yes", client.get(cluster.node(isolatedId).getPort(), "synced"));
    }

    @Test
    public void isolatedFollowerCannotReadWhileLeaderCan() throws Exception {
        cluster = RaftKvClusterHarness.startThreeNodeCluster();
        int leaderId = cluster.leaderId();
        cluster.putOnLeader(client, "base", "v0");

        int followerId = 1;
        while (followerId == leaderId) {
            followerId++;
        }

        FaultInjectionRegistry.isolate(followerId);
        client.put(cluster.leaderPort(), "blocked", "yes");
        Thread.sleep(1500);

        client.waitFor(cluster.leaderPort(), "blocked", "yes", 80);
        client.waitForReadUnavailable(cluster.node(followerId).getPort(), "blocked", 30);

        FaultInjectionRegistry.recover();
        Thread.sleep(2000);
        client.waitFor(cluster.node(followerId).getPort(), "blocked", "yes", 80);
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

    private void assertReadUnavailable(int port, String key) {
        try {
            client.get(port, key);
            fail("Expected linearizable read to fail on isolated node for key: " + key);
        } catch (HttpServerErrorException e) {
            assertEquals(503, e.getRawStatusCode());
        }
    }
}
