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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.ByteString;
import com.song.yama.raft.exception.RaftException;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfChange;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfChangeType;
import com.song.yama.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.EntryType;
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message.Builder;
import com.song.yama.raft.protobuf.RaftProtoBuf.MessageType;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.utils.Utils;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class DefaultNode implements Node {

    private Raft raft;

    private SoftState preSoftState;

    private HardState preHardState;

    public DefaultNode(RaftConfiguration configuration) {
        checkArgument(configuration != null);
        checkArgument(configuration.getId() != 0);
        this.raft = new Raft(configuration);
    }

    /**
     * StartNode returns a new Node given configuration and a list of raft peers. It appends a ConfChangeAddNode entry
     * for each given peer to the initial log.
     */
    public DefaultNode(RaftConfiguration configuration, List<Peer> peers) {
        checkArgument(configuration != null);
        checkArgument(configuration.getId() != 0);
        raft = new Raft(configuration);
        // become the follower at term 1 and apply initial configuration
        // entries of term 1
        raft.becomeFollower(1, 0);
        if (peers == null || peers.isEmpty()) {
            throw new IllegalStateException("Peers can't be null or empty");
        }
        peers.forEach(peer -> {
            ConfChange confChange = ConfChange.newBuilder()
                .setType(ConfChangeType.ConfChangeAddNode).setNodeID(peer.getId())
                .setContext(ByteString.copyFrom(peer.getContext())).build();
            Entry entry = Entry.newBuilder().setType(EntryType.EntryConfChange).setTerm(1)
                .setIndex(raft.getRaftLog().lastIndex() + 1).setData(confChange.toByteString())
                .build();
            raft.getRaftLog().append(Collections.singletonList(entry));
        });

        // Mark these initial entries as committed.
        // TODO(bdarnell): These entries are still unstable; do we need to preserve
        // the invariant that committed < unstable?
        raft.getRaftLog().setCommitted(raft.getRaftLog().lastIndex());

        // Now apply them, mainly so that the application can call Campaign
        // immediately after StartNode in tests. Note that these nodes will
        // be added to raft twice: here and when the application's Ready
        // loop calls ApplyConfChange. The calls to addNode must come after
        // all calls to raftLog.append so progress.next is set after these
        // bootstrapping entries (it is an error if we try to append these
        // entries since they have already been committed).
        // We do not set raftLog.applied so the application will be able
        // to observe all conf changes via Ready.CommittedEntries.
        peers.forEach(peer -> raft.addNode(peer.getId()));

        this.preSoftState = this.raft.softState();
        this.preHardState = HardState.newBuilder().build();
    }

    @Override
    public void tick() {
        this.raft.tick();
    }

    @Override
    public void campaign() {
        this.raft.step(Message.newBuilder().setType(MessageType.MsgHup).build());
    }

    @Override
    public void propose(byte[] data) {
        Builder message = Message.newBuilder()
            .setType(MessageType.MsgProp)
            .setFrom(this.raft.getId())
            .addEntries(Entry.newBuilder()
                .setData(ByteString.copyFrom(data))
                .build());
        this.raft.step(message.build());
    }

    @Override
    public void proposeConfChange(ConfChange confChange) {
        Message message = Message.newBuilder()
            .setType(MessageType.MsgProp)
            .addEntries(Entry.newBuilder()
                .setData(confChange.toByteString())
                .setType(EntryType.EntryConfChange)
                .build())
            .build();
        this.raft.step(message);
    }

    @Override
    public void heartbeatElapsedStep(Message message) {

    }

    @Override
    public Ready pullReady() {
        return new Ready(raft, this.preSoftState, this.preHardState);
    }

    @Override
    public void advance(Ready ready) {
        advanceAppend(ready);

        long commitIdx = this.preHardState.getCommit();
        if (commitIdx != 0) {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            advanceApply(commitIdx);
        }
    }

    public void advanceApply(long applied) {
        commitApply(applied);
    }

    public void advanceAppend(Ready ready) {
        commitReady(ready);
    }

    public void commitReady(Ready ready) {
        if (ready.getSoftState() != null) {
            this.preSoftState = ready.getSoftState();
        }

        HardState hardState = ready.getHardState();
        if (hardState != null && hardState.getCommit() > 0) {
            this.preHardState = hardState;
        }

        if (CollectionUtils.isNotEmpty(ready.getEntries())) {
            Entry entry = ready.getEntries().get(ready.getEntries().size() - 1);
            this.raft.getRaftLog().stableTo(entry.getIndex(), entry.getTerm());
        }

        Snapshot snapshot = ready.getSnapshot();
        if (snapshot != null && snapshot != Snapshot.newBuilder().build()) {
            this.raft.getRaftLog().stableSnapTo(snapshot.getMetadata().getIndex());
        }

        if (CollectionUtils.isNotEmpty(ready.getReadStates())) {
            this.raft.getReadStates().clear();
        }
    }

    public void commitApply(long applied) {
        this.raft.getRaftLog().appliedTo(applied);
    }

    @Override
    public ConfState applyConfChange(ConfChange confChange) {
        if (confChange.getNodeID() == Utils.INVALID_ID) {
            return ConfState.newBuilder()
                .addAllNodes(this.raft.getPrs().keySet())
                .addAllLearners(this.raft.getLearnerPrs().keySet())
                .build();
        }

        long nodeID = confChange.getNodeID();
        ConfChangeType changeType = confChange.getType();
        if (changeType == ConfChangeType.ConfChangeAddNode) {
            this.raft.addNode(nodeID);
        } else if (changeType == ConfChangeType.ConfChangeAddLearnerNode) {
            this.raft.addLearner(nodeID);
        } else if (changeType == ConfChangeType.ConfChangeRemoveNode) {
            this.raft.removeNode(nodeID);
        } else if (changeType == ConfChangeType.ConfChangeUpdateNode) {
            throw new RaftException("Unexpected conf type :" + changeType);
        }
        return ConfState.newBuilder()
            .addAllNodes(this.raft.getPrs().keySet())
            .addAllLearners(this.raft.getLearnerPrs().keySet())
            .build();
    }

    @Override
    public void step(Message message) {
        if (Utils.isLocalMessage(message.getType())) {
            log.warn("Ignore local message :" + message.getType());
            return;
        }
        if (this.raft.getPrs().containsKey(message.getFrom()) ||
            !Utils.isResponseMessage(message.getType())) {
            this.raft.step(message);
        }
        throw new RaftException("Step peer not found : " + message.getType() + ",from :" + message.getFrom());
    }

    @Override
    public void transferLeadership(long lead, long transferee) {
        this.raft.step(Message.newBuilder()
            .setType(MessageType.MsgTransferLeader)
            .setFrom(transferee)
            .build());
    }

    @Override
    public void readIndex(byte[] rctx) {
        this.raft.step(Message.newBuilder()
            .setType(MessageType.MsgReadIndex)
            .addEntries(Entry.newBuilder()
                .setData(ByteString.copyFrom(rctx))
                .build())
            .build());
    }

    @Override
    public Status status() {
        return Status.getStatus(this.raft);
    }

    @Override
    public void reportUnreachable(long id) {
        this.raft.step(Message.newBuilder()
            .setType(MessageType.MsgUnreachable)
            .setFrom(id)
            .build());
    }

    @Override
    public void reportSnapshot(long id, SnapshotStatus snapshotStatus) {
        boolean rej = snapshotStatus == SnapshotStatus.SnapshotFailure;
        this.raft.step(Message.newBuilder()
            .setType(MessageType.MsgSnapStatus)
            .setFrom(id)
            .setReject(rej)
            .build());
    }

    @Override
    public void stop() {

    }
}
