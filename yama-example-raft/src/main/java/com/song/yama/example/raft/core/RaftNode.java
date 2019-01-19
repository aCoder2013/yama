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
import com.song.yama.example.raft.network.MessagingService;
import com.song.yama.example.raft.properties.RaftProperties;
import com.song.yama.example.raft.storage.SnapshotStorage;
import com.song.yama.example.raft.storage.impl.SimpleSnapshotStorage;
import com.song.yama.raft.DefaultNode;
import com.song.yama.raft.MemoryRaftStorage;
import com.song.yama.raft.Node;
import com.song.yama.raft.Peer;
import com.song.yama.raft.RaftConfiguration;
import com.song.yama.raft.RaftStorage;
import com.song.yama.raft.Ready;
import com.song.yama.raft.exception.RaftException;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.protobuf.WALRecord;
import com.song.yama.raft.utils.Utils;
import com.song.yama.raft.wal.CommitLog;
import com.song.yama.raft.wal.RaftStateRecord;
import com.song.yama.raft.wal.RocksDBCommitLog;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
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

    private ExecutorService taskThreadPool = Executors.newFixedThreadPool(2, new ThreadFactory() {
        private AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread thread = new Thread("raft-task-pool-" + counter.incrementAndGet());
            thread.setUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception :" + t, e));
            return thread;
        }
    });

    /* spring managed beans */
    @Autowired
    private StateMachine stateMachine;

    @Autowired
    private RaftProperties raftProperties;

    @Autowired
    private MessagingService messagingService;

    @PostConstruct
    public synchronized void start() throws IOException {
        this.id = this.raftProperties.getId();
        this.peers = new ArrayList<>(Arrays.asList(this.raftProperties.getServers().split(";")));
        this.join = this.raftProperties.isJoin();
        this.stateMachine = stateMachine;
        //maybe this should be rocksdb path?
        this.waldir = String.format(System.getProperty("user.home") + "/yama/data/rocksdb/wal-%d", id);
        this.snapdir = String.format(System.getProperty("user.home") + "/yama/data/snap-%d", id);
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

        this.taskThreadPool.submit(() -> {
            while (true) {
                Ready ready = this.node.pullReady();
                this.commitLog.save(ready.getHardState(), ready.getCommittedEntries());
                if (!Utils.isEmptySnap(ready.getSnapshot())) {
                    saveSnap(ready.getSnapshot());
                    this.raftStorage.applySnapshot(ready.getSnapshot());
                    //publishSnapshot(rd.Snapshot)
                }
                this.raftStorage.append(ready.getEntries());
                this.messagingService.send(ready.getMessages());
                //TODO:biz state machine
//                if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
//                    rc.stop()
//                    return
//                }
//                rc.maybeTriggerSnapshot()
                this.node.advance(ready);
            }
        });
    }

    public void processMessage(Message message) {
        this.node.step(message);
    }

    private synchronized void saveSnap(Snapshot snapshot) {
        WALRecord.Snapshot walSnap = WALRecord.Snapshot.newBuilder()
            .setTerm(snapshot.getMetadata().getTerm())
            .setIndex(snapshot.getMetadata().getIndex())
            .build();
        Result<Void> result = this.commitLog.saveSnap(walSnap);
        if (result.isFailure()) {
            throw new RaftException("save snap failed:" + result.getMessage());
        }

        try {
            this.snapshotStorage.save(snapshot);
        } catch (IOException e) {
            log.error("Save snap failed", e);
            throw new RaftException("Save snap failed:" + e.getMessage());
        }
    }

    private List<Entry> entriesToApply(List<Entry> committedEntries) {
        if (CollectionUtils.isEmpty(committedEntries)) {
            return Collections.emptyList();
        }
        long firstIndex = committedEntries.get(0).getIndex();
        if (firstIndex > this.appliedIndex + 1) {
            throw new RaftException(String
                .format("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIndex,
                    this.appliedIndex));
        }
        if ((this.appliedIndex - firstIndex + 1) < committedEntries.size()) {
            return committedEntries.subList((int) (this.appliedIndex - firstIndex + 1), committedEntries.size());
        }
        return committedEntries;
    }

}
