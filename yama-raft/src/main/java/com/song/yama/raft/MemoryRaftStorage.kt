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

import com.google.protobuf.ByteString
import com.song.yama.common.utils.Result
import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf.*
import com.song.yama.raft.utils.ErrorCode
import com.song.yama.raft.utils.ProtoBufUtils
import com.song.yama.raft.utils.Utils
import org.apache.commons.collections4.CollectionUtils
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 * a simple Raft Storage based memory,just for test.
 */
class MemoryRaftStorage : RaftStorage {

    private val lock = ReentrantLock()

    private var hardState: HardState = HardState.newBuilder().build()

    private var snapshot: Snapshot = Snapshot.newBuilder().build()

    // ents[i] has raft log position i+snapshot.Metadata.Index
    var ents: MutableList<Entry> = listOf(Entry.newBuilder().build()).toMutableList()

    constructor() {}

    constructor(ents: MutableList<Entry>) {
        this.ents = ents
    }

    override fun initialState(): RaftState {
        return RaftState(this.hardState, this.snapshot.metadata.confState)
    }

    override fun entries(low: Long, high: Long, maxSize: Long): Result<List<Entry>> {
        this.lock.lock()
        try {
            val offset = this.ents[0].index
            if (low <= offset) {
                return Result.fail(ErrorCode.ErrCompacted.code, ErrorCode.ErrCompacted.desc)
            }

            if (high > lastIndex() + 1) {
                throw RaftException(
                        String.format("entries' hi(%d) is out of bound lastindex(%d)", high, lastIndex()))
            }

            //on contains dummy entries
            if (ents.size == 1) {
                return Result.fail(ErrorCode.ErrUnavailable.code, ErrorCode.ErrUnavailable.desc)
            }
            val entries = ArrayList(this.ents.subList((low - offset).toInt(), (high - offset).toInt()))
            return Result.success(Utils.limitSize(entries, maxSize))
        } finally {
            this.lock.unlock()
        }
    }

    override fun term(i: Long): Result<Long> {
        this.lock.lock()
        try {
            val offset = this.ents[0].index
            if (i < offset) {
                return Result.fail(ErrorCode.ErrCompacted.code, ErrorCode.ErrCompacted.desc, 0L)
            }

            return if ((i - offset).toInt() >= this.ents.size) {
                Result.fail(ErrorCode.ErrUnavailable.code, ErrorCode.ErrUnavailable.desc, 0L)
            } else Result.success(this.ents[(i - offset).toInt()].term)
        } finally {
            this.lock.unlock()
        }
    }

    override fun lastIndex(): Long {
        this.lock.lock()
        try {
            return this.ents[0].index + this.ents.size - 1
        } finally {
            this.lock.unlock()
        }
    }

    override fun firstIndex(): Long {
        this.lock.lock()
        try {
            return this.ents[0].index + 1
        } finally {
            this.lock.unlock()
        }
    }

    override fun snapshot(): Result<Snapshot> {
        this.lock.lock()
        try {
            return Result.success(this.snapshot)
        } finally {
            this.lock.unlock()
        }
    }

    override fun applySnapshot(snapshot: Snapshot): Result<Void> {
        this.lock.lock()
        try {
            //handle check for old snapshot being applied
            val msIndex = this.snapshot.metadata.index
            val snapIndex = snapshot.metadata.index
            if (msIndex >= snapIndex) {
                return Result.fail(ErrorCode.ErrSnapOutOfDate.code, ErrorCode.ErrSnapOutOfDate.desc)
            }

            this.snapshot = snapshot
            this.ents = ArrayList(listOf(ProtoBufUtils.buildEntry(snapshot.metadata.index, snapshot.metadata.term)))
        } finally {
            this.lock.unlock()
        }
        return Result.success()
    }

    override fun createSnapshot(i: Long, cs: ConfState, data: ByteArray): Result<Snapshot> {
        this.lock.lock()
        try {
            if (i <= this.snapshot.metadata.index) {
                log.info("i:$i,index:${this.snapshot.metadata.index}")
                return Result.fail(ErrorCode.ErrSnapOutOfDate.code, ErrorCode.ErrSnapOutOfDate.desc,
                        Snapshot.getDefaultInstance())
            }

            val offset = this.ents[0].index
            if (i > lastIndex()) {
                throw RaftException(String.format("snapshot %d is out of bound lastindex(%d)", i, lastIndex()))
            }

            val snapBuilder = this.snapshot.toBuilder()
            val metadataBuilder = this.snapshot.metadata.toBuilder()
                    .setIndex(i)
                    .setTerm(this.ents[(i - offset).toInt()].term)
            if (cs != null) {
                metadataBuilder.confState = cs
            }
            snapBuilder.metadata = metadataBuilder.build()
            snapBuilder.data = ByteString.copyFrom(data)
            this.snapshot = snapBuilder.build()
        } finally {
            this.lock.unlock()
        }
        return Result.success(this.snapshot)
    }

    override fun compact(compactIndex: Long): Result<Void> {
        this.lock.lock()
        try {
            val offset = this.ents[0].index
            if (compactIndex <= offset) {
                return Result.fail(ErrorCode.ErrCompacted.code, ErrorCode.ErrCompacted.desc)
            }

            if (compactIndex > lastIndex()) {
                val message = String.format("compact %d is out of bound lastindex(%d)", compactIndex, lastIndex())
                log.warn(message)
                throw RaftException(message)
            }

            val i = (compactIndex - offset).toInt()

            val ents = ArrayList<Entry>(1 + this.ents.size - i)
            ents.add(ProtoBufUtils.buildEntry(this.ents[i].index, this.ents[i].term))
            ents.addAll(this.ents.subList(i + 1, this.ents.size))
            this.ents = ents
        } finally {
            this.lock.unlock()
        }
        return Result.success()
    }

    override fun append(entries: List<Entry>): Result<Void> {
        var entries = entries
        if (CollectionUtils.isEmpty(entries)) {
            return Result.success()
        }
        this.lock.lock()
        try {
            val first = firstIndex()
            val last = entries[0].index + entries.size - 1

            // shortcut if there is no new entry.
            if (last < first) {
                return Result.success()
            }

            // truncate compacted entries
            if (first > entries[0].index) {
                entries = ArrayList(entries.subList((first - entries[0].index).toInt(), entries.size))
            }

            val offset = entries[0].index - this.ents[0].index
            if (this.ents.size > offset) {
                this.ents = ArrayList(this.ents.subList(0, offset.toInt()))
                this.ents.addAll(entries)
            } else if (this.ents.size.toLong() == offset) {
                this.ents.addAll(entries)
            } else {
                throw RaftException(String
                        .format("missing log entry [last: %d, append at: %d]", lastIndex(), entries[0].index))
            }
        } finally {
            this.lock.unlock()
        }
        return Result.success()
    }

    override fun setHardState(hardState: HardState) {
        this.lock.lock()
        try {
            this.hardState = hardState
        } finally {
            this.lock.unlock()
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(MemoryRaftStorage::class.java)
    }
}
