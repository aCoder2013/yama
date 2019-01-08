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

import com.song.yama.example.raft.storage.SnapshotStorage;
import com.song.yama.example.raft.storage.impl.SimpleSnapshotStorage;
import com.song.yama.raft.Node;
import com.song.yama.raft.RaftStorage;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.wal.CommitLog;
import java.io.File;
import java.util.List;

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
    private long lastIndex;

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
        this.waldir = String.format(System.getProperty("user.home") + "yama/data/wal-%d", id);
        this.snapdir = String.format(System.getProperty("user.home") + "yama/data/snap-%d", id);
    }

    public void start() {
        File snap = new File(snapdir);
        if (!snap.exists()) {
            if (!snap.mkdirs()) {
                throw new RuntimeException("Failed to create snap dir :" + snapdir);
            }
        }

        this.snapshotStorage = new SimpleSnapshotStorage(snapdir);
    }
}
