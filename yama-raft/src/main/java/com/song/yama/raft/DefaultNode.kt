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
package com.song.yama.raft

import com.google.common.base.Preconditions.checkArgument
import com.google.protobuf.ByteString
import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf.*
import com.song.yama.raft.utils.Utils
import org.apache.commons.collections4.CollectionUtils
import org.slf4j.LoggerFactory

class DefaultNode : Node {

    private var raft: Raft? = null

    private var preSoftState: SoftState? = null

    private var preHardState: HardState? = null

    constructor(configuration: RaftConfiguration) {
        checkArgument(configuration.id != 0L)
        this.raft = Raft(configuration)
    }

    /**
     * StartNode returns a new Node given configuration and a list of raft peers. It appends a ConfChangeAddNode entry
     * for each given peer to the initial log.
     */
    constructor(configuration: RaftConfiguration, peers: List<Peer>?) {
        checkArgument(configuration.id != 0L)
        raft = Raft(configuration)
        // become the follower at term 1 and apply initial configuration
        // entries of term 1
        raft!!.becomeFollower(1, 0)
        if (peers == null || peers.isEmpty()) {
            throw IllegalStateException("Peers can't be null or empty")
        }
        peers.forEach { peer ->
            val confChange = ConfChange.newBuilder()
                    .setType(ConfChangeType.ConfChangeAddNode).setNodeID(peer.id)
                    .setContext(ByteString.copyFrom(peer.context)).build()
            val entry = Entry.newBuilder()
                    .setType(EntryType.EntryConfChange)
                    .setTerm(1)
                    .setIndex(raft!!.raftLog.lastIndex() + 1)
                    .setData(confChange.toByteString())
                    .build()
            raft!!.raftLog.append(mutableListOf(entry))
        }

        // Mark these initial entries as committed.
        // TODO(bdarnell): These entries are still unstable; do we need to preserve
        // the invariant that committed < unstable?
        raft!!.raftLog.committed = raft!!.raftLog.lastIndex()

        // Now apply them, mainly so that the application can call Campaign
        // immediately after StartNode in tests. Note that these nodes will
        // be added to raft twice: here and when the application's Ready
        // loop calls ApplyConfChange. The calls to addNode must come after
        // all calls to raftLog.append so progress.next is set after these
        // bootstrapping entries (it is an error if we try to append these
        // entries since they have already been committed).
        // We do not set raftLog.applied so the application will be able
        // to observe all conf changes via Ready.CommittedEntries.
        peers.forEach { peer ->
            raft!!.addNode(peer.id)
        }

        this.preSoftState = this.raft!!.softState()
        this.preHardState = HardState.newBuilder().build()
    }

    override fun tick() {
        this.raft!!.tick()
    }

    override fun campaign() {
        this.raft!!.step(Message.newBuilder().setType(MessageType.MsgHup).build())
    }

    override fun propose(data: ByteArray) {
        val message = Message.newBuilder()
                .setType(MessageType.MsgProp)
                .setFrom(this.raft!!.id)
                .addEntries(Entry.newBuilder()
                        .setData(ByteString.copyFrom(data))
                        .build())
        this.raft!!.step(message.build())
    }

    override fun proposeConfChange(confChange: ConfChange) {
        val message = Message.newBuilder()
                .setType(MessageType.MsgProp)
                .addEntries(Entry.newBuilder()
                        .setData(confChange.toByteString())
                        .setType(EntryType.EntryConfChange)
                        .build())
                .build()
        this.raft!!.step(message)
    }

    override fun heartbeatElapsedStep(message: Message) {

    }

    @Throws(InterruptedException::class)
    override fun pullReady(): Ready {
        while (true) {
            val ready = Ready(raft!!, this.preSoftState!!, this.preHardState!!)
            if (ready.containsUpdates()) {
                return ready
            }
            Thread.sleep(1)
            log.info("Sleep!!")
        }
    }

    override fun advance(ready: Ready) {
        advanceAppend(ready)

        val commitIdx = this.preHardState!!.commit
        if (commitIdx != 0L) {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            advanceApply(commitIdx)
        }
    }

    fun advanceApply(applied: Long) {
        commitApply(applied)
    }

    fun advanceAppend(ready: Ready) {
        commitReady(ready)
    }

    fun commitReady(ready: Ready) {
        if (ready.softState != null) {
            this.preSoftState = ready.softState
        }

        val hardState = ready.hardState
        if (hardState.commit > 0) {
            this.preHardState = hardState
        }

        if (CollectionUtils.isNotEmpty(ready.entries)) {
            val entry = ready.entries.get(ready.entries.size - 1)
            this.raft!!.raftLog.stableTo(entry.index, entry.term)
        }

        val snapshot = ready.snapshot
        if (snapshot != null && snapshot !== Snapshot.newBuilder().build()) {
            this.raft!!.raftLog.stableSnapTo(snapshot.metadata.index)
        }

        if (CollectionUtils.isNotEmpty(ready.readStates)) {
            this.raft!!.readStates.clear()
        }
    }

    fun commitApply(applied: Long) {
        this.raft!!.raftLog.appliedTo(applied)
    }

    override fun applyConfChange(confChange: ConfChange): ConfState {
        if (confChange.nodeID == Utils.INVALID_ID) {
            return ConfState.newBuilder()
                    .addAllNodes(this.raft!!.prs.keys)
                    .addAllLearners(this.raft!!.learnerPrs.keys)
                    .build()
        }

        val nodeID = confChange.nodeID
        val changeType = confChange.type
        if (changeType == ConfChangeType.ConfChangeAddNode) {
            this.raft!!.addNode(nodeID)
        } else if (changeType == ConfChangeType.ConfChangeAddLearnerNode) {
            this.raft!!.addLearner(nodeID)
        } else if (changeType == ConfChangeType.ConfChangeRemoveNode) {
            this.raft!!.removeNode(nodeID)
        } else if (changeType == ConfChangeType.ConfChangeUpdateNode) {
            throw RaftException("Unexpected conf type :$changeType")
        }
        return ConfState.newBuilder()
                .addAllNodes(this.raft!!.prs.keys)
                .addAllLearners(this.raft!!.learnerPrs.keys)
                .build()
    }

    override fun step(message: Message) {
        if (Utils.isLocalMessage(message.type)) {
            log.warn("Ignore local message :" + message.type)
            return
        }
        if (this.raft!!.prs.containsKey(message.from) || !Utils.isResponseMessage(message.type)) {
            this.raft!!.step(message)
            return
        }
        throw RaftException(
                "Step peer not found : " + message.type + ",from :" + message.from + ",peers:" + this.raft!!
                        .prs.keys)
    }

    override fun transferLeadership(lead: Long, transferee: Long) {
        this.raft!!.step(Message.newBuilder()
                .setType(MessageType.MsgTransferLeader)
                .setFrom(transferee)
                .build())
    }

    override fun readIndex(rctx: ByteArray) {
        this.raft!!.step(Message.newBuilder()
                .setType(MessageType.MsgReadIndex)
                .addEntries(Entry.newBuilder()
                        .setData(ByteString.copyFrom(rctx))
                        .build())
                .build())
    }

    override fun status(): Status {
        return Status.getStatus(this.raft!!)
    }

    override fun reportUnreachable(id: Long) {
        this.raft!!.step(Message.newBuilder()
                .setType(MessageType.MsgUnreachable)
                .setFrom(id)
                .build())
    }

    override fun reportSnapshot(id: Long, snapshotStatus: SnapshotStatus) {
        val rej = snapshotStatus === SnapshotStatus.SnapshotFailure
        this.raft!!.step(Message.newBuilder()
                .setType(MessageType.MsgSnapStatus)
                .setFrom(id)
                .setReject(rej)
                .build())
    }

    override fun stop() {

    }

    companion object {
        private val log = LoggerFactory.getLogger(DefaultNode::class.java)
    }
}
