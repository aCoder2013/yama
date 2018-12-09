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

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.song.yama.raft;


import com.song.yama.raft.protobuf.RaftProtoBuf;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfChange;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;

/**
 * Node represents a node in a raft cluster
 */
public interface Node {

    /**
     * Tick increments the internal logical clock for the Node by a single tick. Election timeouts and heartbeat
     * timeouts are in units of ticks
     */
    void tick();

    /**
     * Campaign causes the Node to transition to candidate state and start campaigning to become leader
     */
    void campaign();

    /**
     * Propose proposes that data be appended to the log
     */
    void propose(byte[] data);

    /**
     * ProposeConfChange proposes config change. At most one ConfChange can be in the process of going through
     * consensus. Application needs to call ApplyConfChange when applying EntryConfChange type entry.
     */
    void proposeConfChange(RaftProtoBuf.ConfChange confChange);

    /**
     * Step advances the state machine using the given message. ctx.Err() will be returned, if any
     */
    void heartbeatElapsedStep(RaftProtoBuf.Message message);


    /**
     * Ready returns a channel that returns the current point-in-time state. Users of the Node must call Advance after
     * retrieving the state returned by Ready.
     *
     * NOTE: No committed entries from the next Ready may be applied until all committed entries and snapshots from the
     * previous one have finished.
     */
    Ready pullReady();

    /**
     * Advance notifies the Node that the application has saved progress up to the last Ready. It prepares the node to
     * return the next available Ready.
     *
     * The application should generally call Advance after it applies the entries in last Ready.
     *
     * However, as an optimization, the application may call Advance while it is applying the commands. For example.
     * when the last Ready contains a snapshot, the application might take a long time to apply the snapshot data. To
     * continue receiving Ready without blocking raft progress, it can call Advance before finishing applying the last
     * ready.
     */
    void advance(Ready ready);

    /**
     * ApplyConfChange applies config change to the local node. Returns an opaque ConfState protobuf which must be
     * recorded in snapshots. Will never return nil; it returns a pointer only to match MemoryStorage.Compact.
     */
    ConfState applyConfChange(ConfChange confChange);

    void step(Message message);

    /**
     * TransferLeadership attempts to transfer leadership to the given transferee.
     */
    void transferLeadership(long lead, long transferee);

    /**
     * ReadIndex request a read state. The read state will be set in the ready. Read state has a read index. Once the
     * application advances further than the read index, any linearizable read requests issued before the read request
     * can be processed safely. The read state will have the same rctx attached.
     */
    void readIndex(byte[] rctx);

    /**
     * Status returns the current status of the raft state machine
     */
    Status status();

    /**
     * reportUnreachable reports the given node is not reachable for the last send.
     */
    void reportUnreachable(long id);

    /**
     * ReportSnapshot reports the status of the sent snapshot
     */
    void reportSnapshot(long id, SnapshotStatus snapshotStatus);

    /**
     * Stop performs any necessary termination of the Node.
     */
    void stop();
}
