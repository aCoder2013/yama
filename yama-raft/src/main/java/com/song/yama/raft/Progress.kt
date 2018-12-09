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

import com.song.yama.raft.exception.RaftException

/**
 * Progress represents a follower’s progress in the view of the leader. Leader maintains progresses of all followers,
 * and sends entries to the follower based on its progress.
 */
class Progress {

    var match: Long = 0

    var next: Long = 0

    /**
     * State defines how the leader should interact with the follower.
     *
     * When in ProgressStateProbe, leader sends at most one replication message per heartbeat interval. It also probes
     * actual progress of the follower.
     *
     * When in ProgressStateReplicate, leader optimistically increases next to the latest entry sent after sending
     * replication message. This is an optimized state for fast replicating log entries to the follower.
     *
     * When in ProgressStateSnapshot, leader should have sent out snapshot before and stops sending any replication
     * message.
     */
    var state: ProgressStateType = ProgressStateType.ProgressStateProbe

    /**
     * Paused is used in ProgressStateProbe. When Paused is true, raft should pause sending replication message to this
     * peer.
     */
    var paused: Boolean = false

    /**
     * PendingSnapshot is used in ProgressStateSnapshot. If there is a pending snapshot, the pendingSnapshot will be set
     * to the index of the snapshot. If pendingSnapshot is set, the replication process of this Progress will be paused.
     * raft will not resend snapshot until the pending one is reported to be failed.
     */
    var pendingSnapshot: Long = 0

    /**
     * RecentActive is true if the progress is recently active. Receiving any messages from the corresponding follower
     * indicates the progress is active. RecentActive can be reset to false after an election timeout.
     */
    var isRecentActive: Boolean = false

    /**
     * inflights is a sliding window for the inflight messages. Each inflight message contains one or more log entries.
     * The max number of entries per message is defined in raft config as MaxSizePerMsg. Thus inflight effectively
     * limits both the number of inflight messages and the bandwidth each Progress can use. When inflights is full, no
     * more message should be sent. When a leader sends out a message, the index of the last entry should be added to
     * inflights. The index MUST be added into inflights in order. When a leader receives a reply, the previous
     * inflights should be freed by calling inflights.freeTo with the index of the last received entry.
     */
    var ins: Inflight = Inflight()

    /**
     * IsLearner is true if this progress is tracked for a learner.
     */
    var isLearner: Boolean = false

    constructor() {}


    constructor(match: Long, next: Long) {
        this.match = match
        this.next = next
    }


    constructor(next: Long, match: Long, ins: Inflight) {
        this.match = match
        this.next = next
        this.ins = ins
    }


    constructor(state: ProgressStateType, match: Long, next: Long) {
        this.match = match
        this.next = next
        this.state = state
    }

    @JvmOverloads
    constructor(next: Long, ins: Inflight, isLearner: Boolean = false) {
        this.next = next
        this.ins = ins
        this.isLearner = isLearner
    }

    constructor(next: Long, match: Long, ins: Inflight, isLearner: Boolean) {
        this.match = match
        this.next = next
        this.ins = ins
        this.isLearner = isLearner
    }

    constructor(match: Long, next: Long, state: ProgressStateType, ins: Inflight) {
        this.match = match
        this.next = next
        this.state = state
        this.ins = ins
    }

    constructor(match: Long, next: Long, state: ProgressStateType, pendingSnapshot: Long, ins: Inflight) {
        this.match = match
        this.next = next
        this.state = state
        this.pendingSnapshot = pendingSnapshot
        this.ins = ins
    }

    constructor(state: ProgressStateType, isPaused: Boolean, ins: Inflight) {
        this.state = state
        this.paused = isPaused
        this.ins = ins
    }

    constructor(next: Long, paused: Boolean) {
        this.next = next
        this.paused = paused
    }

    constructor(isLearner: Boolean) {
        this.isLearner = isLearner
    }

    fun resetState(state: ProgressStateType) {
        this.paused = false
        this.pendingSnapshot = 0
        this.state = state
        ins.reset()
    }

    fun becomeSnapshot(snapshoti: Long) {
        resetState(ProgressStateType.ProgressStateSnapshot)
        pendingSnapshot = snapshoti
    }

    fun becomeProbe() {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (this.state == ProgressStateType.ProgressStateSnapshot) {
            val pendingSnapshot = this.pendingSnapshot
            resetState(ProgressStateType.ProgressStateProbe)
            this.next = Math.max(this.match + 1, pendingSnapshot + 1)
        } else {
            resetState(ProgressStateType.ProgressStateProbe)
            this.next = this.match + 1
        }
    }

    fun becomeReplicate() {
        resetState(ProgressStateType.ProgressStateReplicate)
        this.next = this.match + 1
    }

    fun optimisticUpdate(n: Long) {
        this.next = n + 1
    }

    /**
     * maybeUpdate returns false if the given n index comes from an outdated message. Otherwise it updates the progress
     * and returns true.
     */
    fun maybeUpdate(n: Long): Boolean {
        var updated = false
        if (match < n) {
            match = n
            updated = true
            resume()
        }

        if (next < n + 1) {
            next = n + 1
        }
        return updated
    }

    /**
     * maybeDecrTo returns false if the given to index comes from an out of order message. Otherwise it decreases the
     * progress next index to min(rejected, last) and returns true.
     */
    fun maybeDecrTo(rejected: Long, last: Long): Boolean {
        if (state == ProgressStateType.ProgressStateReplicate) {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if (rejected <= match) {
                return false
            }
            // directly decrease next to match + 1
            next = match + 1
            return true
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if (this.next - 1 != rejected) {
            return false
        }

        this.next = Math.min(rejected, last + 1)
        if (next < 1) {
            this.next = 1
        }

        resume()
        return true
    }

    fun pause() {
        this.paused = true
    }

    fun resume() {
        paused = false
    }

    /**
     * IsPaused returns whether sending log entries to this node has been
     * paused. A node may be paused because it has rejected recent
     * MsgApps, is currently waiting for a snapshot, or has reached the
     * MaxInflightMsgs limit.
     */
    fun isPaused(): Boolean {
        return when (this.state) {
            ProgressStateType.ProgressStateProbe -> {
                this.paused
            }

            ProgressStateType.ProgressStateReplicate -> {
                this.ins.isFull()
            }
            ProgressStateType.ProgressStateSnapshot -> {
                true
            }
            else -> {
                throw RaftException("Unknown state type")
            }
        }
    }

    fun snapshotFailure() {
        this.pendingSnapshot = 0
    }

    /**
     * needSnapshotAbort returns true if snapshot progress's Match
     * is equal or higher than the pendingSnapshot.
     */
    fun needSnapshotAbort(): Boolean = this.state == ProgressStateType.ProgressStateSnapshot && this.match >= this.pendingSnapshot

    override fun toString(): String {
        return "Progress(match=$match, next=$next, state=$state, paused=$paused, pendingSnapshot=$pendingSnapshot, isRecentActive=$isRecentActive, ins=$ins, isLearner=$isLearner)"
    }

    enum class ProgressStateType {
        ProgressStateProbe, ProgressStateReplicate, ProgressStateSnapshot
    }
}
