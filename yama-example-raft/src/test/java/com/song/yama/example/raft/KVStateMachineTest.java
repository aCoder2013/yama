package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
import com.song.yama.example.raft.support.JsonKvTestValues;
import com.song.yama.example.raft.support.TestKvStateMachine;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.EntryType;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.protobuf.RaftProtoBuf.SnapshotMetadata;
import org.junit.Test;

public class KVStateMachineTest {

    @Test
    public void processCommitsAndLookup() {
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits("{\"key\":\"k\",\"value\":\"v\"}");
        assertEquals("v", sm.lookup("k"));
    }

    @Test
    public void snapshotRoundTrip() {
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits("{\"key\":\"a\",\"value\":\"1\"}");
        sm.processCommits("{\"key\":\"b\",\"value\":\"2\"}");

        byte[] data = sm.getSnapshot();
        Snapshot snapshot = Snapshot.newBuilder()
            .setMetadata(SnapshotMetadata.newBuilder().setIndex(5).setTerm(2).build())
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .build();

        TestKvStateMachine restored = new TestKvStateMachine();
        restored.loadSnapshot(snapshot);
        assertEquals("1", restored.lookup("a"));
        assertEquals("2", restored.lookup("b"));
    }

    @Test
    public void applyEntryIgnoresConfChange() {
        TestKvStateMachine sm = new TestKvStateMachine();
        Entry confEntry = Entry.newBuilder()
            .setType(EntryType.EntryConfChange)
            .setIndex(1)
            .setTerm(1)
            .build();
        sm.applyEntry(confEntry);
        assertEquals(0, sm.size());
    }

    @Test
    public void loadNullSnapshotIsNoOp() {
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits("{\"key\":\"x\",\"value\":\"y\"}");
        sm.loadSnapshot(null);
        assertEquals("y", sm.lookup("x"));
    }

    @Test
    public void jsonFormatMatchesApplication() {
        JSONObject json = new JSONObject();
        json.put("key", "name");
        json.put("value", "alice");
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits(json.toJSONString());
        assertNotNull(sm.lookup("name"));
        assertNull(sm.lookup("missing"));
    }

    @Test
    public void unicodeAndSpecialCharsRoundTrip() {
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits(buildCommit("unicode", JsonKvTestValues.UNICODE_VALUE));
        sm.processCommits(buildCommit("quoted", JsonKvTestValues.QUOTED_VALUE));
        sm.processCommits(buildCommit("escaped", JsonKvTestValues.ESCAPED_VALUE));
        sm.processCommits(buildCommit("json-like", JsonKvTestValues.JSON_LIKE_VALUE));
        sm.processCommits(buildCommit("empty", JsonKvTestValues.EMPTY_VALUE));

        assertEquals(JsonKvTestValues.UNICODE_VALUE, sm.lookup("unicode"));
        assertEquals(JsonKvTestValues.QUOTED_VALUE, sm.lookup("quoted"));
        assertEquals(JsonKvTestValues.ESCAPED_VALUE, sm.lookup("escaped"));
        assertEquals(JsonKvTestValues.JSON_LIKE_VALUE, sm.lookup("json-like"));
        assertEquals(JsonKvTestValues.EMPTY_VALUE, sm.lookup("empty"));
    }

    @Test
    public void snapshotPreservesSpecialJsonValues() {
        TestKvStateMachine sm = new TestKvStateMachine();
        sm.processCommits(buildCommit("unicode", JsonKvTestValues.UNICODE_VALUE));
        sm.processCommits(buildCommit("escaped", JsonKvTestValues.ESCAPED_VALUE));

        byte[] data = sm.getSnapshot();
        Snapshot snapshot = Snapshot.newBuilder()
            .setMetadata(SnapshotMetadata.newBuilder().setIndex(3).setTerm(1).build())
            .setData(com.google.protobuf.ByteString.copyFrom(data))
            .build();

        TestKvStateMachine restored = new TestKvStateMachine();
        restored.loadSnapshot(snapshot);
        assertEquals(JsonKvTestValues.UNICODE_VALUE, restored.lookup("unicode"));
        assertEquals(JsonKvTestValues.ESCAPED_VALUE, restored.lookup("escaped"));
    }

    private static String buildCommit(String key, String value) {
        JSONObject json = new JSONObject();
        json.put("key", key);
        json.put("value", value);
        return json.toJSONString();
    }
}
