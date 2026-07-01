package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.song.yama.example.raft.support.KvRaftCluster;
import com.song.yama.example.raft.support.KvRaftTestNode;
import com.song.yama.example.raft.support.TestKvStateMachine;
import org.junit.Test;

public class KvRaftFaultTest {

    @Test
    public void threeNodeReplication() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "name", "alice");
        cluster.sync();
        cluster.assertKvConsistent("name", "alice");
    }

    @Test
    public void asymmetricDropBlocksReplication() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "seed", "v0");
        cluster.sync();

        cluster.drop(1, 2, 2.0f);
        cluster.drop(1, 3, 2.0f);
        cluster.put(1, "blocked", "yes");
        cluster.sync();

        assertEquals("yes", cluster.node(1).getKv().lookup("blocked"));
        assertNull(cluster.node(2).getKv().lookup("blocked"));
        assertNull(cluster.node(3).getKv().lookup("blocked"));
    }

    @Test
    public void partitionHealKvConverges() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);

        cluster.drop(1, 2, 2.0f);
        cluster.drop(1, 3, 2.0f);
        cluster.put(1, "during", "partition");
        cluster.sync();

        cluster.recover();
        cluster.heartbeat(1);
        cluster.put(1, "after", "heal");
        cluster.sync();

        cluster.assertKvConsistent("during", "partition");
        cluster.assertKvConsistent("after", "heal");
    }

    @Test
    public void leaderFailoverPreservesWrites() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "k1", "v1");
        cluster.sync();

        cluster.isolate(1);
        cluster.electLeader(2);
        cluster.put(2, "k2", "v2");
        cluster.sync();

        assertEquals("v1", cluster.node(2).getKv().lookup("k1"));
        assertEquals("v2", cluster.node(2).getKv().lookup("k2"));
        assertEquals("v2", cluster.node(3).getKv().lookup("k2"));
    }

    @Test
    public void minorityPartitionCannotElectLeader() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(5);
        cluster.electLeader(1);

        cluster.cut(4, 1);
        cluster.cut(4, 2);
        cluster.cut(4, 3);
        cluster.cut(5, 1);
        cluster.cut(5, 2);
        cluster.cut(5, 3);

        cluster.electLeader(4);
        cluster.electLeader(5);

        assertNotEquals(4L, cluster.leaderId());
        assertNotEquals(5L, cluster.leaderId());
    }

    @Test
    public void oldLeaderRejoinsAndKvConverges() {
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

        cluster.assertKvConsistent("a", "1");
        cluster.assertKvConsistent("b", "2");
        long leaders = java.util.Arrays.stream(new long[]{1, 2, 3})
            .filter(id -> cluster.node(id).isLeader())
            .count();
        assertEquals(1, leaders);
    }

    @Test
    public void manyWritesStayConsistent() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        for (int i = 0; i < 15; i++) {
            cluster.put(1, "key-" + i, "value-" + i);
        }
        cluster.sync();
        for (int i = 0; i < 15; i++) {
            cluster.assertKvConsistent("key-" + i, "value-" + i);
        }
        assertTrue(cluster.node(1).getKv().size() >= 15);
    }

    @Test
    public void followerReadMayBeStaleBeforeSync() {
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
        assertEquals("yes", cluster.node(2).getKv().lookup("synced"));
    }
}
