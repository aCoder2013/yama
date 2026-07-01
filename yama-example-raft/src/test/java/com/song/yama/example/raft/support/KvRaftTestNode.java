package com.song.yama.example.raft.support;

import com.google.common.collect.Lists;
import com.song.yama.raft.MemoryRaftStorage;
import com.song.yama.raft.Raft;
import com.song.yama.raft.RaftConfiguration;
import com.song.yama.raft.StateType;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import java.util.ArrayList;
import java.util.List;

/**
 * Single Raft peer with an attached KV state machine for deterministic tests.
 */
public class KvRaftTestNode {

    private final long id;
    private final Raft raft;
    private final MemoryRaftStorage storage;
    private final TestKvStateMachine kv;

    public KvRaftTestNode(long id, List<Long> peerIds) {
        this.id = id;
        this.storage = new MemoryRaftStorage();
        RaftConfiguration config = new RaftConfiguration();
        config.setId(id);
        config.setPeers(peerIds);
        config.setElectionTick(10);
        config.setHeartbeatTick(1);
        config.setRaftStorage(storage);
        config.setMaxSizePerMsg(Long.MAX_VALUE);
        config.setMaxInflightMsgs(256);
        this.raft = new Raft(config);
        this.kv = new TestKvStateMachine();
    }

    public long getId() {
        return id;
    }

    public Raft getRaft() {
        return raft;
    }

    public MemoryRaftStorage getStorage() {
        return storage;
    }

    public TestKvStateMachine getKv() {
        return kv;
    }

    public boolean isLeader() {
        return raft.getState() == StateType.LEADER;
    }

    public void step(Message message) {
        raft.step(message);
    }

    public List<Message> drainMessages() {
        List<Message> msgs = new ArrayList<>(raft.getMsgs());
        raft.setMsgs(Lists.newArrayList());
        return msgs;
    }

    public void applyCommitted() {
        storage.append(raft.getRaftLog().unstableEntries());
        raft.getRaftLog().stableTo(raft.getRaftLog().lastIndex(), raft.getRaftLog().lastTerm());
        List<Entry> entries = raft.getRaftLog().nextEnts();
        for (Entry entry : entries) {
            kv.applyEntry(entry);
        }
        raft.getRaftLog().appliedTo(raft.getRaftLog().getCommitted());
    }

    public void propose(String key, String value) {
        kv.propose(raft, key, value);
    }
}
