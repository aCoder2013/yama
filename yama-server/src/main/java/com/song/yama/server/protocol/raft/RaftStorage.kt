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

package com.song.yama.server.protocol.raft

import com.song.yama.common.utils.Result
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.*

interface RaftStorage {

    /**
     * InitialState returns the saved HardState and ConfState information
     */
    fun initialState(): RaftState

    /**
     * Entries returns a slice of log entries in the range [lo,hi). MaxSize limits the total size of the log entries
     * returned, but Entries returns at least one entry if any
     */
    fun entries(low: Long, high: Long, maxSize: Long): Result<List<Entry>>

    /**
     * Term returns the term of entry i, which must be in the range [FirstIndex()-1, LastIndex()]. The term of the entry
     * before FirstIndex is retained for matching purposes even though the rest of that entry may not be available
     */
    fun term(i: Long): Result<Long>

    /**
     * LastIndex returns the index of the last entry in the log
     */
    fun lastIndex(): Long

    /**
     * FirstIndex returns the index of the first log entry that is possibly available via Entries (older entries have
     * been incorporated into the latest Snapshot; if storage only contains the dummy entry the first log entry is not
     * available)
     */
    fun firstIndex(): Long

    /**
     * Snapshot returns the most recent snapshot. If snapshot is temporarily unavailable, it should return
     * ErrSnapshotTemporarilyUnavailable, so raft state machine could know that Storage needs some time to prepare
     * snapshot and call Snapshot later.
     */
    fun snapshot(): Result<Snapshot>

    /**
     * ApplySnapshot overwrites the contents of this Storage object with those of the given sna
     */
    fun applySnapshot(snapshot: Snapshot): Result<Void>

    /**
     * CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and can be used to reconstruct the state
     * at that point. If any configuration changes have been made since the last compaction, the result of the last
     * ApplyConfChange must be passed in.
     */
    fun createSnapshot(i: Long, cs: ConfState, data: ByteArray): Result<Snapshot>

    /**
     * Compact discards all log entries prior to compactIndex. It is the application's responsibility to not attempt to
     * compact an index greater than raftLog.applied.
     */
    fun compact(compactIndex: Long): Result<Void>


    /**
     * Append the new entries to storage. TODO (xiangli): ensure the entries are continuous and entries[0].Index >
     * ms.entries[0].Index
     */
    fun append(entries: List<Entry>): Result<Void>

    /**
     * SetHardState saves the current HardState.
     */
    fun setHardState(hardState: HardState)
}

