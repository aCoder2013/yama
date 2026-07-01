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
import com.song.yama.raft.ReadState;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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

    private static final long DEFAULT_SNAPSHOT_COUNT = 10;

    private static final long SNAPSHOT_CATCHUP_ENTRIES_N = 10;

    private static final long READ_INDEX_TIMEOUT_MS = 5000L;

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

    private volatile long appliedIndex;

    private final Object appliedIndexLock = new Object();

    private final Object raftLock = new Object();

    private final BlockingQueue<Message> inboundMessages = new LinkedBlockingQueue<>();

    private final BlockingQueue<byte[]> pendingReadIndexRequests = new LinkedBlockingQueue<>();

    private final ConcurrentMap<String, CompletableFuture<Long>> pendingReadIndexes = new ConcurrentHashMap<>();

    private long snapCount = DEFAULT_SNAPSHOT_COUNT;

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
        String dataBase = StringUtils.isNotBlank(this.raftProperties.getDataDir())
            ? this.raftProperties.getDataDir()
            : System.getProperty("user.home") + "/yama/data";
        this.waldir = String.format(dataBase + "/rocksdb/wal-%d", id);
        FileUtils.forceMkdir(new File(this.waldir));
        this.snapdir = String.format(dataBase + "/snap-%d", id);
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
            this.stateMachine.loadSnapshot(snapshot);
            List<Entry> ents = raftStateRecord.getEnts();
            if (CollectionUtils.isNotEmpty(ents)) {
                this.raftStorage.append(ents);
                this.lastIndex = ents.get(ents.size() - 1).getIndex();
            }
        } else {
            Result<RaftStateRecord> raftStateRecordResult = this.commitLog.readAll();
            if (raftStateRecordResult.isSuccess() && raftStateRecordResult.getData() != null) {
                RaftStateRecord raftStateRecord = raftStateRecordResult.getData();
                if (raftStateRecord.getHardState() != null) {
                    this.raftStorage.setHardState(raftStateRecord.getHardState());
                }
                List<Entry> ents = raftStateRecord.getEnts();
                if (CollectionUtils.isNotEmpty(ents)) {
                    this.raftStorage.append(ents);
                    this.lastIndex = ents.get(ents.size() - 1).getIndex();
                    replayEntriesToStateMachine(ents);
                }
            }
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
        boolean hasExistingState = snapshot != null || this.lastIndex > 0;
        if (hasExistingState) {
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
        log.info("update appliedIndex:{}.", this.appliedIndex);

        this.scheduledExecutorService
            .scheduleAtFixedRate(() -> {
                synchronized (this.raftLock) {
                    this.node.tick();
                }
                this.inboundMessages.offer(Message.getDefaultInstance());
            }, 100, 100, TimeUnit.MILLISECONDS);

        this.running = true;
        this.taskThreadPool.submit(new ReadyProcessor(this));
    }

    public void processMessage(Message message) {
        this.inboundMessages.offer(message);
    }

    /**
     * Perform a linearizable read by issuing readIndex and waiting until the state machine
     * has applied entries up to the confirmed read index.
     */
    public <T> T linearizableRead(Supplier<T> readAction) {
        byte[] requestCtx = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        String requestKey = new String(requestCtx, StandardCharsets.UTF_8);
        CompletableFuture<Long> readIndexFuture = new CompletableFuture<>();
        pendingReadIndexes.put(requestKey, readIndexFuture);
        this.pendingReadIndexRequests.offer(requestCtx);
        this.inboundMessages.offer(Message.getDefaultInstance());
        try {
            long readIndex = readIndexFuture.get(READ_INDEX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            waitUntilApplied(readIndex);
            return readAction.get();
        } catch (TimeoutException e) {
            throw new RaftException("ReadIndex timed out waiting for quorum confirmation", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RaftException("ReadIndex interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RaftException("ReadIndex failed", e.getCause());
        } finally {
            pendingReadIndexes.remove(requestKey);
        }
    }

    private void drainRaftWork() {
        Message message;
        while ((message = this.inboundMessages.poll()) != null) {
            if (message.getSerializedSize() > 0) {
                this.node.step(message);
            }
        }
        byte[] requestCtx;
        while ((requestCtx = this.pendingReadIndexRequests.poll()) != null) {
            this.node.readIndex(requestCtx);
        }
    }

    private void waitUntilApplied(long readIndex) throws InterruptedException, TimeoutException {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(READ_INDEX_TIMEOUT_MS);
        synchronized (this.appliedIndexLock) {
            while (this.appliedIndex < readIndex) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new TimeoutException(
                        "Timed out waiting for appliedIndex >= " + readIndex + ", current=" + this.appliedIndex);
                }
                long waitMillis = remainingNanos / 1_000_000L;
                int waitNanos = (int) (remainingNanos % 1_000_000L);
                this.appliedIndexLock.wait(waitMillis, waitNanos);
            }
        }
    }

    private void notifyAppliedIndexUpdated() {
        synchronized (this.appliedIndexLock) {
            this.appliedIndexLock.notifyAll();
        }
    }

    private void processReadStates(List<ReadState> readStates) {
        if (CollectionUtils.isEmpty(readStates)) {
            return;
        }
        for (ReadState readState : readStates) {
            String requestKey = new String(readState.getRequestCtx(), StandardCharsets.UTF_8);
            CompletableFuture<Long> future = pendingReadIndexes.get(requestKey);
            if (future != null) {
                future.complete(readState.getIndex());
            }
        }
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
                    this.confState = this.node.applyConfChange(confChange);
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

            // after commit, update appliedIndex
            this.appliedIndex = entry.getIndex();
            log.info("Update appliedIndex:{}.", this.appliedIndex);
            notifyAppliedIndexUpdated();
        });
    }

    private void replayEntriesToStateMachine(List<Entry> entries) {
        entries.forEach(entry -> {
            if (entry.getType() == EntryType.EntryNormal) {
                if (entry.getData() != null && !entry.getData().isEmpty()) {
                    this.stateMachine.processCommits(entry.getData().toStringUtf8());
                }
            }
            this.appliedIndex = entry.getIndex();
            notifyAppliedIndexUpdated();
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

        this.stateMachine.loadSnapshot(snapshotToSave);
        this.confState = snapshotToSave.getMetadata().getConfState();
        this.snapshotIndex = snapshotToSave.getMetadata().getIndex();
        this.appliedIndex = snapshotToSave.getMetadata().getIndex();
        log.info("Update appliedIndex:{}.", this.appliedIndex);
        notifyAppliedIndexUpdated();
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

    private void maybeTriggerSnapshot() {
        if (this.appliedIndex - this.snapshotIndex <= this.snapCount) {
            return;
        }

        log.info("start snapshot [applied index: {} | last snapshot index: {}]", this.appliedIndex, this.snapshotIndex);
        byte[] snapshotData = this.stateMachine.getSnapshot();
        Result<Snapshot> snapshotResult = this.raftStorage
            .createSnapshot(this.appliedIndex, this.confState, snapshotData);
        if (snapshotResult.isFailure()) {
            log.error("Failed to create snapshot:" + snapshotResult.getMessage());
            return;
        }
        saveSnap(snapshotResult.getData());
        long compactIndex = 1L;
        if (this.appliedIndex > SNAPSHOT_CATCHUP_ENTRIES_N) {
            compactIndex = this.appliedIndex - SNAPSHOT_CATCHUP_ENTRIES_N;
        }
        Result<Void> result = this.raftStorage.compact(compactIndex);
        if (result.isFailure()) {
            log.error("Failed to compact : " + result.getMessage());
        }

        log.info("compacted log at index :{}.", compactIndex);
        this.snapshotIndex = this.appliedIndex;
    }

    @PreDestroy
    public void close() throws IOException {
        running = false;
        this.scheduledExecutorService.shutdownNow();
        this.taskThreadPool.shutdownNow();
        try {
            this.taskThreadPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.commitLog.close();
    }

    public class ReadyProcessor implements Runnable {

        private final RaftNode raftNode;

        ReadyProcessor(RaftNode raftNode) {
            this.raftNode = raftNode;
        }

        @Override
        public void run() {
            while (raftNode.running) {
                try {
                    Ready ready;
                    synchronized (raftNode.raftLock) {
                        raftNode.drainRaftWork();
                        ready = raftNode.node.tryPullReady();
                        if (ready == null) {
                            continue;
                        }
                        raftNode.commitLog.save(ready.getHardState(), ready.getCommittedEntries());
                        if (!Utils.INSTANCE.isEmptySnap(ready.getSnapshot())) {
                            saveSnap(Objects.requireNonNull(ready.getSnapshot()));
                            raftNode.raftStorage.applySnapshot(ready.getSnapshot());
                            publishSnapshot(ready.getSnapshot());
                        }
                        raftNode.raftStorage.append(ready.getEntries());
                        raftNode.messagingService.send(ready.getMessages());
                        processReadStates(ready.getReadStates());
                        publishEntries(entriesToApply(ready.getCommittedEntries()));
                        maybeTriggerSnapshot();
                        raftNode.node.advance(ready);
                    }
                } catch (Exception e) {
                    log.error("Process ready failed", e);
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
