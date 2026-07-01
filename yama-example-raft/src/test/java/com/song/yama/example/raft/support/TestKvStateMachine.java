package com.song.yama.example.raft.support;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.protobuf.ByteString;
import com.song.yama.raft.Raft;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.EntryType;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import com.song.yama.raft.protobuf.RaftProtoBuf.MessageType;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * In-memory KV state machine for Raft fault tests (no Spring).
 */
public class TestKvStateMachine {

    private ConcurrentMap<String, String> kvStorage = new ConcurrentHashMap<>();

    public void propose(Raft raft, String key, String value) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", key);
        jsonObject.put("value", value);
        Message message = Message.newBuilder()
            .setType(MessageType.MsgProp)
            .setFrom(raft.getId())
            .addEntries(Entry.newBuilder()
                .setData(ByteString.copyFrom(jsonObject.toJSONString().getBytes()))
                .build())
            .build();
        raft.step(message);
    }

    public void applyEntry(Entry entry) {
        if (entry.getType() != EntryType.EntryNormal || entry.getData() == null || entry.getData().isEmpty()) {
            return;
        }
        processCommits(entry.getData().toStringUtf8());
    }

    public void processCommits(String commit) {
        JSONObject kv = JSONObject.parseObject(commit);
        kvStorage.put(kv.getString("key"), kv.getString("value"));
    }

    public String lookup(String key) {
        return kvStorage.get(key);
    }

    public byte[] getSnapshot() {
        return JSONObject.toJSONString(kvStorage).getBytes();
    }

    public void loadSnapshot(Snapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        kvStorage = JSONObject.parseObject(snapshot.getData().toByteArray(),
            new TypeReference<ConcurrentMap<String, String>>() {
            }.getType());
    }

    public int size() {
        return kvStorage.size();
    }
}
