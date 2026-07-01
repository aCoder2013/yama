package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.song.yama.example.raft.support.KvRaftCluster;
import com.song.yama.example.raft.support.ReadIndexKvTestHelper;
import org.junit.Test;

/**
 * Fault tests for ReadIndex linearizable reads using the in-memory KV Raft harness.
 */
public class ReadIndexKvRaftFaultTest {

    @Test
    public void linearizableReadReturnsCommittedValueOnAllNodes() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "name", "alice");
        cluster.sync();

        for (long nodeId = 1; nodeId <= 3; nodeId++) {
            assertEquals("alice", ReadIndexKvTestHelper.linearizableLookup(cluster, nodeId, "name"));
        }
    }

    @Test
    public void directLookupMayBeStaleButReadIndexIsLinearizable() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "synced", "yes");
        cluster.sync();

        cluster.isolate(2);
        cluster.put(1, "newkey", "newval");
        cluster.sync();

        assertEquals("newval", cluster.node(1).getKv().lookup("newkey"));
        assertEquals("newval", cluster.node(3).getKv().lookup("newkey"));
        assertNull(cluster.node(2).getKv().lookup("newkey"));

        assertEquals("newval", ReadIndexKvTestHelper.linearizableLookup(cluster, 1, "newkey"));
        assertEquals("newval", ReadIndexKvTestHelper.linearizableLookup(cluster, 3, "newkey"));
        ReadIndexKvTestHelper.assertLinearizableReadUnavailable(cluster, 2, "newkey");
        ReadIndexKvTestHelper.assertLinearizableReadUnavailable(cluster, 2, "synced");
    }

    @Test
    public void partitionHealRestoresLinearizableRead() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "seed", "v0");
        cluster.sync();

        cluster.isolate(2);
        cluster.put(1, "after", "partition");
        cluster.sync();

        ReadIndexKvTestHelper.assertLinearizableReadUnavailable(cluster, 2, "after");

        cluster.recover();
        cluster.heartbeat(1);
        cluster.sync();

        assertEquals("partition", ReadIndexKvTestHelper.linearizableLookup(cluster, 2, "after"));
        assertEquals("v0", ReadIndexKvTestHelper.linearizableLookup(cluster, 2, "seed"));
    }

    @Test
    public void leaderFailoverPreservesLinearizableRead() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "k1", "v1");
        cluster.sync();

        cluster.isolate(1);
        cluster.electLeader(2);
        cluster.put(2, "k2", "v2");
        cluster.sync();

        assertEquals("v1", ReadIndexKvTestHelper.linearizableLookup(cluster, 2, "k1"));
        assertEquals("v2", ReadIndexKvTestHelper.linearizableLookup(cluster, 3, "k2"));
        ReadIndexKvTestHelper.assertLinearizableReadUnavailable(cluster, 1, "k2");
    }

    @Test
    public void asymmetricDropBlocksLinearizableReadOnFollower() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "seed", "v0");
        cluster.sync();

        cluster.drop(2, 1, 1.0f);
        cluster.drop(1, 2, 1.0f);
        cluster.put(1, "blocked", "yes");
        cluster.sync();

        assertEquals("yes", ReadIndexKvTestHelper.linearizableLookup(cluster, 1, "blocked"));
        ReadIndexKvTestHelper.assertLinearizableReadUnavailable(cluster, 2, "blocked");
    }

    @Test
    public void oldLeaderRejoinsLinearizableReadConverges() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "a", "1");
        cluster.sync();

        cluster.isolate(1);
        cluster.electLeader(2);
        cluster.put(2, "b", "2");
        cluster.sync();

        cluster.recover();
        cluster.heartbeat(2);
        cluster.sync();

        for (long nodeId = 1; nodeId <= 3; nodeId++) {
            assertEquals("1", ReadIndexKvTestHelper.linearizableLookup(cluster, nodeId, "a"));
            assertEquals("2", ReadIndexKvTestHelper.linearizableLookup(cluster, nodeId, "b"));
        }
    }
}
