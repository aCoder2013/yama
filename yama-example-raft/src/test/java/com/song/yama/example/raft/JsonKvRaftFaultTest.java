package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.song.yama.example.raft.support.JsonKvTestValues;
import com.song.yama.example.raft.support.KvRaftCluster;
import org.junit.Test;

/**
 * Fault tests verifying JSON-encoded KV payloads stay consistent under network failures.
 */
public class JsonKvRaftFaultTest {

    @Test
    public void unicodeValuesSurviveReplication() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "unicode", JsonKvTestValues.UNICODE_VALUE);
        cluster.sync();
        cluster.assertKvConsistent("unicode", JsonKvTestValues.UNICODE_VALUE);
    }

    @Test
    public void specialCharactersSurvivePartitionHeal() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "quoted", JsonKvTestValues.QUOTED_VALUE);
        cluster.sync();

        cluster.isolate(2);
        cluster.put(1, "escaped", JsonKvTestValues.ESCAPED_VALUE);
        cluster.sync();

        assertEquals(JsonKvTestValues.ESCAPED_VALUE, cluster.node(1).getKv().lookup("escaped"));
        assertNull(cluster.node(2).getKv().lookup("escaped"));

        cluster.recover();
        cluster.heartbeat(1);
        cluster.sync();

        cluster.assertKvConsistent("quoted", JsonKvTestValues.QUOTED_VALUE);
        cluster.assertKvConsistent("escaped", JsonKvTestValues.ESCAPED_VALUE);
    }

    @Test
    public void jsonLikeValueSurvivesLeaderFailover() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "payload", JsonKvTestValues.JSON_LIKE_VALUE);
        cluster.sync();

        cluster.isolate(1);
        cluster.electLeader(2);
        cluster.put(2, "unicode", JsonKvTestValues.UNICODE_VALUE);
        cluster.sync();

        assertEquals(JsonKvTestValues.JSON_LIKE_VALUE, cluster.node(2).getKv().lookup("payload"));
        assertEquals(JsonKvTestValues.UNICODE_VALUE, cluster.node(3).getKv().lookup("unicode"));
    }

    @Test
    public void emptyValueSurvivesAsymmetricDrop() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "seed", "v0");
        cluster.sync();

        cluster.drop(1, 2, 2.0f);
        cluster.drop(1, 3, 2.0f);
        cluster.put(1, "empty", JsonKvTestValues.EMPTY_VALUE);
        cluster.sync();

        assertEquals(JsonKvTestValues.EMPTY_VALUE, cluster.node(1).getKv().lookup("empty"));
        assertNull(cluster.node(2).getKv().lookup("empty"));
        assertNull(cluster.node(3).getKv().lookup("empty"));
    }

    @Test
    public void manySpecialJsonWritesStayConsistent() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);

        String[] values = {
            JsonKvTestValues.UNICODE_VALUE,
            JsonKvTestValues.QUOTED_VALUE,
            JsonKvTestValues.ESCAPED_VALUE,
            JsonKvTestValues.JSON_LIKE_VALUE,
            JsonKvTestValues.EMPTY_VALUE
        };

        for (int i = 0; i < values.length; i++) {
            cluster.put(1, "json-" + i, values[i]);
        }
        cluster.sync();

        for (int i = 0; i < values.length; i++) {
            cluster.assertKvConsistent("json-" + i, values[i]);
        }
    }

    @Test
    public void oldLeaderRejoinsJsonValuesConverge() {
        KvRaftCluster cluster = KvRaftCluster.newCluster(3);
        cluster.electLeader(1);
        cluster.put(1, "a", JsonKvTestValues.QUOTED_VALUE);
        cluster.sync();

        cluster.isolate(1);
        cluster.electLeader(2);
        cluster.put(2, "b", JsonKvTestValues.ESCAPED_VALUE);
        cluster.sync();

        cluster.recover();
        cluster.heartbeat(2);
        cluster.sync();

        cluster.assertKvConsistent("a", JsonKvTestValues.QUOTED_VALUE);
        cluster.assertKvConsistent("b", JsonKvTestValues.ESCAPED_VALUE);
    }
}
