package com.song.yama.example.raft.support;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Shared fault-injection rules across multiple Spring Boot nodes in integration tests.
 */
public final class FaultInjectionRegistry {

    private static final Map<Connection, Float> DROP_RATES = new ConcurrentHashMap<>();

    private FaultInjectionRegistry() {
    }

    public static void drop(long from, long to, float rate) {
        DROP_RATES.put(new Connection(from, to), rate);
    }

    public static void cut(long one, long other) {
        drop(one, other, 2.0f);
        drop(other, one, 2.0f);
    }

    public static void isolate(long nodeId) {
        for (long peer = 1; peer <= 5; peer++) {
            if (peer != nodeId) {
                drop(nodeId, peer, 1.0f);
                drop(peer, nodeId, 1.0f);
            }
        }
    }

    public static void recover() {
        DROP_RATES.clear();
    }

    public static boolean shouldDrop(long from, long to) {
        float rate = DROP_RATES.getOrDefault(new Connection(from, to), 0f);
        if (rate <= 0f) {
            return false;
        }
        return ThreadLocalRandom.current().nextFloat() < rate;
    }

    private static final class Connection {
        private final long from;
        private final long to;

        private Connection(long from, long to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Connection)) {
                return false;
            }
            Connection that = (Connection) o;
            return from == that.from && to == that.to;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(from) * 31 + Long.hashCode(to);
        }
    }
}
