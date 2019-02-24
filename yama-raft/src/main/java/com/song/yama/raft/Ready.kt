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

import com.song.yama.raft.protobuf.RaftProtoBuf.*
import com.song.yama.raft.utils.Utils
import com.song.yama.raft.utils.Utils.isEmptyHardState
import com.song.yama.raft.utils.Utils.isEmptySnap
import org.apache.commons.collections4.CollectionUtils
import java.util.*
import kotlin.collections.ArrayList

/**
 * Ready encapsulates the entries and messages that are ready to read, be saved to stable storage, committed or sent to
 * other peers. All fields in Ready are read-only.
 */
class Ready(val raft: Raft, val preSoftSt: SoftState, val preHardSt: HardState) {

    /**
     * The current volatile state of a Node. SoftState will be nil if there is no update. It is not required to consume
     * or store SoftState.
     */
    var softState: SoftState? = null

    /**
     * The current state of a Node to be saved to stable storage BEFORE Messages are sent. HardState will be equal to
     * empty state if there is no update.
     */
    var hardState: HardState = HardState.getDefaultInstance()

    /**
     * ReadStates can be used for node to serve linearizable read requests locally when its applied index is greater
     * than the index in ReadState. Note that the readState will be returned when raft receives msgReadIndex. The
     * returned is only valid for the request that requested to read.
     */
    var readStates: MutableList<ReadState> = Collections.emptyList()

    /**
     * Entries specifies entries to be saved to stable storage BEFORE Messages are sent.
     */
    var entries: List<Entry> = Collections.emptyList()

    /**
     * Snapshot specifies the snapshot to be saved to stable storage
     */
    var snapshot: Snapshot? = null

    /**
     * Messages specifies outbound messages to be sent AFTER Entries are committed to stable storage. If it contains a
     * MsgSnap message, the application MUST report back to raft when the snapshot has been received or has failed by
     * calling ReportSnapshot.
     */
    val committedEntries: List<Entry>


    /**
     * Messages specifies outbound messages to be sent AFTER Entries are committed to stable storage. If it contains a
     * MsgSnap message, the application MUST report back to raft when the snapshot has been received or has failed by
     * calling ReportSnapshot.
     */
    var messages: List<Message>? = null

    /**
     * MustSync indicates whether the HardState and Entries must be synchronously written to disk or if an asynchronous
     * write is permissible.
     */
    val mustSync: Boolean

    init {
        this.entries = raft.raftLog.unstableEntries()
        this.committedEntries = raft.raftLog.nextEnts()
        if (CollectionUtils.isNotEmpty(raft.msgs)) {
            this.messages = ArrayList(raft.msgs)
            raft.msgs = ArrayList()
        } else {
            this.messages = emptyList()
        }
        val softSt = raft.softState()
        if (softSt != preSoftSt) {
            this.softState = softSt
        }

        val hardSt = raft.hardState()
        if (!Utils.isHardStateEqual(hardSt, preHardSt)) {
            this.hardState = hardSt

            if (CollectionUtils.isNotEmpty(this.committedEntries)) {
                val lastCommit = this.committedEntries[committedEntries.size - 1]
                if (this.hardState.commit > lastCommit.index) {
                    this.hardState = this.hardState.toBuilder().setCommit(lastCommit.index).build()
                }
            }
        }

        if (raft.raftLog.unstable.snapshot != null) {
            this.snapshot = raft.raftLog.unstable.snapshot
        }

        if (CollectionUtils.isNotEmpty(raft.readStates)) {
            this.readStates = raft.readStates
            this.readStates.clear()
        }

        this.mustSync = mustSync(raft.hardState(), preHardSt, this.entries.size)
    }

    fun containsUpdates(): Boolean {
        return (this.softState != null || !isEmptyHardState(this.hardState) ||
                !isEmptySnap(this.snapshot) || CollectionUtils.isNotEmpty(this.entries) ||
                CollectionUtils.isNotEmpty(committedEntries) || CollectionUtils
                .isNotEmpty(this.messages)
                || CollectionUtils.isNotEmpty(this.readStates))
    }

    companion object {

        fun mustSync(st: HardState, prevst: HardState, entsnum: Int): Boolean {
            return entsnum != 0 || st.vote != prevst.vote || st.term != prevst.term
        }
    }
}
