/*
 *  Copyright 2018 acoder2013
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http:www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.song.yama.example.raft.core;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class kVStateMachine implements StateMachine {

    private ConcurrentMap<String, String> kvStorage = new ConcurrentHashMap<>();

    @Autowired
    private RaftNode raftNode;

    @Override
    public void propose(String key, String value) {
        log.info("Propose key:{},value:{}.", key, value);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("key", key);
        jsonObject.put("value", value);
        this.raftNode.getNode().propose(jsonObject.toJSONString().getBytes());
    }

    @Override
    public String lookup(String key) {
        return this.kvStorage.get(key);
    }

    @Override
    public void processCommits(String commit) {
        JSONObject kv = JSONObject.parseObject(commit);
        log.info("Finish to propose commits :{}.", kv.toJSONString());
        this.kvStorage.put(kv.getString("key"), kv.getString("value"));
    }

    @Override
    public void loadSnapshot() {
        Snapshot snapshot = this.raftNode.getSnapshotStorage().load();
        if (snapshot == null) {
            return;
        }
        log.info("loading snapshot at term {} and index {}", snapshot.getMetadata().getTerm(),
            snapshot.getMetadata().getIndex());
        recoverFromSnapshot(snapshot.getData().toByteArray());
    }

    private void recoverFromSnapshot(byte[] snapshot) {
        this.kvStorage = JSONObject.parseObject(snapshot, new TypeReference<ConcurrentMap<String, String>>() {
        }.getType());
    }
}
