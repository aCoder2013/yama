package com.song.yama.example.raft.support;

import com.song.yama.raft.exception.RaftException;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import com.song.yama.raft.protobuf.RaftProtoBuf.MessageType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * In-memory multi-node KV Raft cluster with message loss / partition simulation.
 */
public class KvRaftCluster {

    private final Map<Long, KvRaftTestNode> nodes;
    private final Map<Connection, Float> dropRates = new HashMap<>();

    private KvRaftCluster(Map<Long, KvRaftTestNode> nodes) {
        this.nodes = nodes;
    }

    public static KvRaftCluster newCluster(int size) {
        List<Long> peerIds = IntStream.rangeClosed(1, size).asLongStream().boxed().collect(Collectors.toList());
        Map<Long, KvRaftTestNode> nodeMap = new HashMap<>();
        for (Long id : peerIds) {
            nodeMap.put(id, new KvRaftTestNode(id, peerIds));
        }
        return new KvRaftCluster(nodeMap);
    }

    public KvRaftTestNode node(long id) {
        return nodes.get(id);
    }

    public void electLeader(long id) {
        send(single(Message.newBuilder().setFrom(id).setTo(id).setType(MessageType.MsgHup).build()));
    }

    public void heartbeat(long leaderId) {
        send(single(Message.newBuilder().setFrom(leaderId).setTo(leaderId).setType(MessageType.MsgBeat).build()));
    }

    public void put(long fromNodeId, String key, String value) {
        nodes.get(fromNodeId).propose(key, value);
        sync();
    }

    public void sync() {
        for (KvRaftTestNode node : nodes.values()) {
            List<Message> pending = node.drainMessages();
            if (!pending.isEmpty()) {
                send(pending);
            }
        }
        nodes.values().forEach(KvRaftTestNode::applyCommitted);
    }

    public void send(List<Message> messages) {
        List<Message> queue = new ArrayList<>(messages);
        while (!queue.isEmpty()) {
            List<Message> next = new ArrayList<>();
            for (Message message : queue) {
                KvRaftTestNode target = nodes.get(message.getTo());
                if (target == null) {
                    throw new RaftException(message.getTo() + " doesn't exist");
                }
                target.step(message);
                next.addAll(filter(target.drainMessages()));
            }
            queue = next;
        }
        nodes.values().forEach(KvRaftTestNode::applyCommitted);
    }

    public void drop(long from, long to, float rate) {
        dropRates.put(new Connection(from, to), rate);
    }

    public void cut(long one, long other) {
        drop(one, other, 2.0f);
        drop(other, one, 2.0f);
    }

    public void isolate(long id) {
        for (long peerId : nodes.keySet()) {
            if (peerId != id) {
                drop(id, peerId, 1.0f);
                drop(peerId, id, 1.0f);
            }
        }
    }

    public void recover() {
        dropRates.clear();
    }

    public long leaderId() {
        return nodes.values().stream()
            .filter(KvRaftTestNode::isLeader)
            .map(KvRaftTestNode::getId)
            .findFirst()
            .orElse(-1L);
    }

    public void assertKvConsistent(String key, String expected) {
        for (KvRaftTestNode node : nodes.values()) {
            String actual = node.getKv().lookup(key);
            if (expected == null ? actual != null : !expected.equals(actual)) {
                throw new AssertionError(String.format("node %d: key=%s expected=%s actual=%s",
                    node.getId(), key, expected, actual));
            }
        }
    }

    private List<Message> filter(List<Message> messages) {
        List<Message> kept = new ArrayList<>();
        for (Message message : messages) {
            if (message.getType() == MessageType.MsgHup) {
                throw new RaftException("unexpected msgHup on the wire");
            }
            float rate = dropRates.getOrDefault(new Connection(message.getFrom(), message.getTo()), 0f);
            if (ThreadLocalRandom.current().nextFloat() < rate) {
                continue;
            }
            kept.add(message);
        }
        return kept;
    }

    private static List<Message> single(Message message) {
        List<Message> list = new ArrayList<>();
        list.add(message);
        return list;
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
