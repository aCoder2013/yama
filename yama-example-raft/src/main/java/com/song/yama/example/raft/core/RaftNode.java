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

import com.song.yama.common.utils.Result;
import com.song.yama.example.raft.storage.SnapshotStorage;
import com.song.yama.example.raft.storage.impl.SimpleSnapshotStorage;
import com.song.yama.raft.DefaultNode;
import com.song.yama.raft.MemoryRaftStorage;
import com.song.yama.raft.Node;
import com.song.yama.raft.Peer;
import com.song.yama.raft.RaftConfiguration;
import com.song.yama.raft.RaftStorage;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.wal.CommitLog;
import com.song.yama.raft.wal.RaftStateRecord;
import com.song.yama.raft.wal.RocksDBCommitLog;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class RaftNode {

    /**
     * client ID for raft session
     */
    private int id;

    /**
     * raft peer URLs
     */
    private List<String> peers;

    /**
     * node is joining an existing cluster
     */
    private boolean join;

    /**
     * path to WAL directory
     */
    private String waldir;

    /**
     * path to snapshot directory
     */
    private String snapdir;

    private StateMachine stateMachine;

    /**
     * index of log at start
     */
    private volatile long lastIndex;

    private ConfState confState;

    private long snapshotIndex;

    private long appliedIndex;

    /* raft */
    private Node node;

    private RaftStorage raftStorage;

    private CommitLog commitLog;

    private SnapshotStorage snapshotStorage;

    public RaftNode(int id, List<String> peers, boolean join, StateMachine stateMachine) {
        this.id = id;
        this.peers = peers;
        this.join = join;
        this.stateMachine = stateMachine;
        //maybe this should be rocksdb path?
        this.waldir = String.format(System.getProperty("user.home") + "/yama/data/rocksdb/wal-%d", id);
        this.snapdir = String.format(System.getProperty("user.home") + "/yama/data/snap-%d", id);
    }

    public synchronized void start() throws IOException {
        File snap = new File(snapdir);
        if (!snap.exists()) {
            if (!snap.mkdirs()) {
                throw new RuntimeException("Failed to create snap dir :" + snapdir);
            }
        }

        this.snapshotStorage = new SimpleSnapshotStorage(snapdir);
        this.commitLog = new RocksDBCommitLog(this.waldir);
        this.raftStorage = new MemoryRaftStorage();
        log.info("Try to read write ahead log,node:{}.", this.id);
        Snapshot snapshot = this.snapshotStorage.load();
        if (snapshot != null) {
            Result<RaftStateRecord> raftStateRecordResult = this.commitLog.readAll(snapshot);
            if (raftStateRecordResult.isFailure()) {
                throw new IOException("Failed to read wal :" + raftStateRecordResult.getMessage());
            }

            RaftStateRecord raftStateRecord = raftStateRecordResult.getData();
            this.raftStorage.applySnapshot(snapshot);
            this.raftStorage.setHardState(raftStateRecord.getHardState());
            List<Entry> ents = raftStateRecord.getEnts();
            if (CollectionUtils.isNotEmpty(ents)) {
                this.raftStorage.append(ents);
                this.lastIndex = ents.get(ents.size() - 1).getIndex();
            }
            //TODO: maybe trigger commit
        }

        List<Peer> rpeers = new ArrayList<>();
        for (int i = 0; i < this.peers.size(); i++) {
            rpeers.add(new Peer(i + 1));
        }

        RaftConfiguration raftConfiguration = new RaftConfiguration();
        raftConfiguration.setId(this.id);
        raftConfiguration.setElectionTick(10);
        raftConfiguration.setHeartbeatTick(1);
        raftConfiguration.setRaftStorage(this.raftStorage);
        raftConfiguration.setMaxSizePerMsg(1024 * 1024);
        raftConfiguration.setMaxInflightMsgs(256);
        if (snapshot != null) {
            this.node = new DefaultNode(raftConfiguration);
        } else {
            List<Peer> startPeers = rpeers;
            if (this.join) {
                startPeers = Collections.emptyList();
            }
            this.node = new DefaultNode(raftConfiguration, startPeers);
        }

        //TODO:add network module

    }
}
