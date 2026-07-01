package com.song.yama.example.raft.support;

import com.song.yama.raft.ReadState;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Helper for exercising ReadIndex linearizable reads in in-memory KV Raft tests.
 */
public final class ReadIndexKvTestHelper {

    private static final int MAX_ROUNDS = 200;

    private ReadIndexKvTestHelper() {
    }

    public static String linearizableLookup(KvRaftCluster cluster, long nodeId, String key) {
        KvRaftTestNode node = cluster.node(nodeId);
        byte[] requestCtx = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        node.requestReadIndex(requestCtx);

        long readIndex = waitForReadIndex(cluster, node, requestCtx);
        waitUntilApplied(cluster, node, readIndex);
        return node.getKv().lookup(key);
    }

    public static void assertLinearizableReadUnavailable(KvRaftCluster cluster, long nodeId, String key) {
        try {
            linearizableLookup(cluster, nodeId, key);
            throw new AssertionError("Expected ReadIndex to fail on node " + nodeId + " for key " + key);
        } catch (ReadIndexUnavailableException expected) {
            // expected
        }
    }

    private static long waitForReadIndex(KvRaftCluster cluster, KvRaftTestNode node, byte[] requestCtx) {
        for (int round = 0; round < MAX_ROUNDS; round++) {
            cluster.sync();
            ReadState readState = node.pollReadState(requestCtx);
            if (readState != null) {
                return readState.getIndex();
            }
        }
        throw new ReadIndexUnavailableException(
            "ReadIndex timed out on node " + node.getId() + " after " + MAX_ROUNDS + " rounds");
    }

    private static void waitUntilApplied(KvRaftCluster cluster, KvRaftTestNode node, long readIndex) {
        for (int round = 0; round < MAX_ROUNDS; round++) {
            if (node.getAppliedIndex() >= readIndex) {
                return;
            }
            cluster.sync();
        }
        throw new ReadIndexUnavailableException(String.format(
            "Applied index did not reach read index on node %d (applied=%d, readIndex=%d)",
            node.getId(), node.getAppliedIndex(), readIndex));
    }

    public static final class ReadIndexUnavailableException extends RuntimeException {
        public ReadIndexUnavailableException(String message) {
            super(message);
        }
    }
}
