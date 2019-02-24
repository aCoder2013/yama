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

import com.google.protobuf.InvalidProtocolBufferException;
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
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfChange;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfChangeType;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.EntryType;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RaftNode {

    /**
     * client ID for raft session
     */
    @Getter
    private int id;

    /**
     * raft peer URLs
     */
    @Getter
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
    @Getter
    private Node node;

    private RaftStorage raftStorage;

    private CommitLog commitLog;

    @Getter
    private SnapshotStorage snapshotStorage;

    private ExecutorService taskThreadPool = Executors
        .newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new BasicThreadFactory.Builder()
            .namingPattern("yama-raft-task-pool-")
            .uncaughtExceptionHandler((t, e) -> log.info("Uncaught exception : " + t, e))
            .build());

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean running;

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
        this.waldir = String.format(System.getProperty("user.home") + "/yama/data/rocksdb/wal-%d", id);
        FileUtils.forceMkdir(new File(this.waldir));
        this.snapdir = String.format(System.getProperty("user.home") + "/yama/data/snap-%d", id);
        FileUtils.forceMkdir(new File(this.snapdir));

        this.snapshotStorage = new SimpleSnapshotStorage(snapdir);
        this.commitLog = new RocksDBCommitLog(this.waldir);
        this.raftStorage = new MemoryRaftStorage();
        log.info("Try to read write ahead log,node:{}.", this.id);
        Snapshot snapshot = this.snapshotStorage.load();
        if (snapshot != null) {
            Result<RaftStateRecord> raftStateRecordResult = this.commitLog.readAll(snapshot);
            if (raftStateRecordResult.isFailure() || raftStateRecordResult.getData() == null) {
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
//        raftConfiguration.setPreVote(true);
        if (snapshot != null) {
            this.node = new DefaultNode(raftConfiguration);
        } else {
            List<Peer> startPeers = rpeers;
            if (this.join) {
                startPeers = Collections.emptyList();
            }
            this.node = new DefaultNode(raftConfiguration, startPeers);
        }

        Result<Snapshot> snap = this.raftStorage.snapshot();
        if (snap.isFailure()) {
            throw new RaftException("Load snap failed:" + snap.getMessage());
        }
        this.confState = snap.getData().getMetadata().getConfState();
        this.snapshotIndex = snap.getData().getMetadata().getIndex();
        this.appliedIndex = snap.getData().getMetadata().getIndex();

        this.scheduledExecutorService
            .scheduleAtFixedRate(this.node::tick, 100, 100,
                TimeUnit.MILLISECONDS);

        this.running = true;
        this.taskThreadPool.submit(new ReadyProcessor(this));
    }

    public void processMessage(Message message) {
        this.node.step(message);
    }

    private void publishEntries(List<Entry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return;
        }
        entries.forEach(entry -> {
            if (entry.getType() == EntryType.EntryNormal) {
                if (entry.getData() == null || entry.getData().isEmpty()) {
                } else {
                    String content = entry.getData().toStringUtf8();
                    this.stateMachine.processCommits(content);
                }
            } else if (entry.getType() == EntryType.EntryConfChange) {
                try {
                    ConfChange confChange = ConfChange.newBuilder().mergeFrom(entry.getData().toByteArray()).build();
                    this.node.applyConfChange(confChange);
                    //TODO:support cluster node change
                    if (confChange.getType() == ConfChangeType.ConfChangeAddNode) {
                        if (confChange.hasContext()) {
                            String context = confChange.getContext().toStringUtf8();
                            if (StringUtils.isNotBlank(context)) {
                                this.peers.add(context);
                                this.messagingService.refreshHosts(this.peers);
                            }
                        } else {
                            log.warn("Ignore empty context node :{}.", confChange);
                        }
                    } else if (confChange.getType() == ConfChangeType.ConfChangeRemoveNode) {
                        if (confChange.getNodeID() == this.id) {
                            log.info("Oops,I've been removed from raft cluster! Shutting down.");
                            System.exit(1);
                            return;
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.error("Decode ConfChange failed", e);
                    throw new RaftException("Invalid ConfChange record", e);
                }
            }

            this.appliedIndex = entry.getIndex();
            if (entry.getIndex() == this.lastIndex) {
                this.stateMachine.loadSnapshot();
            }
        });
    }

    private void publishSnapshot(Snapshot snapshotToSave) {
        if (Utils.INSTANCE.isEmptySnap(snapshotToSave)) {
            return;
        }
        log.info("publishing snapshot at index {}", this.snapshotIndex);
        if (snapshotToSave.getMetadata().getIndex() <= this.appliedIndex) {
            throw new RaftException(String.format("snapshot index [%d] should > progress.appliedIndex [%d]",
                snapshotToSave.getMetadata().getIndex(), this.appliedIndex));
        }

        this.stateMachine.loadSnapshot();
        this.confState = snapshotToSave.getMetadata().getConfState();
        this.snapshotIndex = snapshotToSave.getMetadata().getIndex();
        this.appliedIndex = snapshotToSave.getMetadata().getIndex();
        log.info("publishing snapshot at index {}", this.snapshotIndex);
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

    @PostConstruct
    public void close() {
        running = false;
        this.taskThreadPool.shutdown();
    }

    public class ReadyProcessor implements Runnable {

        private final RaftNode raftNode;

        ReadyProcessor(RaftNode raftNode) {
            this.raftNode = raftNode;
        }

        @Override
        public void run() {
            while (true){
                try {
                    Ready ready = raftNode.node.pullReady();
                    raftNode.commitLog.save(ready.getHardState(), ready.getCommittedEntries());
                    if (!Utils.INSTANCE.isEmptySnap(ready.getSnapshot())) {
                        saveSnap(ready.getSnapshot());
                        raftNode.raftStorage.applySnapshot(ready.getSnapshot());
                        publishSnapshot(ready.getSnapshot());
                    }
                    raftNode.raftStorage.append(ready.getEntries());
                    raftNode.messagingService.send(ready.getMessages());
                    publishEntries(entriesToApply(ready.getCommittedEntries()));
                    //TODO:when to trigger snapshot
//                rc.maybeTriggerSnapshot()
                    raftNode.node.advance(ready);
                } catch (Exception e) {
                    log.error("Process ready failed", e);
                }
            }
        }
    }
}
