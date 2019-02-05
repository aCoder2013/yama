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


import com.song.yama.common.utils.Result
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot
import org.apache.commons.collections4.CollectionUtils
import org.slf4j.LoggerFactory
import java.util.*


/**
 * unstable.entries[i] has raft log position i+unstable.offset. Note that unstable.offset may be
 * less than the highest log position in storage; this means that the next write to storage might
 * need to truncate the log before persisting unstable.entries.
 */
class Unstable {

    /**
     * the incoming unstable snapshot, if any
     */
    var snapshot: Snapshot? = null

    /**
     * all entries that have not yet been written to storage
     */
    var entries: MutableList<Entry> = ArrayList()

    var offset: Long = 0

    constructor() {}

    constructor(offset: Long) {
        this.offset = offset
    }

    /**
     * maybeFirstIndex returns the index of the first possible entry in entries if it has a
     * snapshot.
     */
    fun maybeFirstIndex(): Result<Long> {
        return if (snapshot != null) {
            Result.success(snapshot!!.metadata.index + 1)
        } else Result.fail(0L)
    }

    /**
     * maybeLastIndex returns the last index if it has at least one unstable entry or snapshot.
     */
    fun maybeLastIndex(): Result<Long> {
        if (CollectionUtils.isNotEmpty(this.entries)) {
            return Result.success(this.offset + this.entries.size - 1)
        }

        return if (this.snapshot != null) {
            Result.success(this.snapshot!!.metadata.index)
        } else Result.fail(0L)

    }

    /**
     * maybeTerm returns the term of the entry at index i, if there is any.
     */
    fun maybeTerm(i: Long): Result<Long> {
        if (i < this.offset) {
            if (this.snapshot == null) {
                return Result.fail(0L)
            }

            return if (this.snapshot!!.metadata.index == i) {
                Result.success(this.snapshot!!.metadata.term)
            } else Result.fail(0L)

        }

        val result = maybeLastIndex()
        if (result.isFailure) {
            return Result.fail(0L)
        }

        return if (i > result.data) {
            Result.fail(0L)
        } else Result.success(this.entries[(i - this.offset).toInt()].term)

    }

    fun stableTo(i: Long, t: Long) {
        val result = maybeTerm(i)
        if (result.isFailure) {
            return
        }

        // if i < offset, term is matched with the snapshot
        // only update the unstable entries if term is matched with
        // an unstable entry.
        if (result.data == t && i >= this.offset) {
            this.entries = this.entries.subList((i + 1 - this.offset).toInt(), this.entries.size)
            this.offset = i + 1
            shrinkEntriesArray()
        }

    }

    /**
     * shrinkEntriesArray discards the underlying array used by the entries slice if most of it
     * isn't being used. This avoids holding references to a bunch of potentially large entries that
     * aren't needed anymore. Simply clearing the entries wouldn't be safe because clients might
     * still be using them.
     */
    fun shrinkEntriesArray() {
        // We replace the array if we're using less than half of the space in
        // it. This number is fairly arbitrary, chosen as an attempt to balance
        // memory usage vs number of allocations. It could probably be improved
        // with some focused tuning.
        //TODO:clean up resources
        //        int lenMultiple = 2;
        //        if(CollectionUtils.isEmpty(this.entries)){
        //            this.entries = new ArrayList<>();
        //        }else if(){
        //
        //        }
    }

    fun stableSnapTo(i: Long) {
        if (this.snapshot != null && this.snapshot!!.metadata.index == i) {
            this.snapshot = null
        }
    }

    fun restore(snapshot: RaftProtoBuf.Snapshot) {
        this.offset = snapshot.metadata.index + 1
        this.entries = ArrayList()
        this.snapshot = snapshot
    }

    fun truncateAndAppend(ents: MutableList<RaftProtoBuf.Entry>) {
        val after = ents[0].index
        if (after == offset + entries.size) {
            // after is the next index in the u.entries
            // directly append
            this.entries.addAll(ents)
        } else if (after <= offset) {
            log.info("replace the unstable entries from index {}", after)
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            this.offset = after
            entries = ents
        } else {
            // truncate to after and copy to u.entries
            // then append
            log.info("truncate the unstable entries before index {}", after)
            this.entries = slice(this.offset, after)
            this.entries.addAll(ents)
        }
    }

    fun slice(lo: Long, hi: Long): MutableList<Entry> {
        mustCheckOutOfBounds(lo, hi)
        return ArrayList(entries.subList((lo - this.offset).toInt(), (hi - offset).toInt()))
    }

    fun mustCheckOutOfBounds(lo: Long, hi: Long) {
        if (lo > hi) {
            throw IllegalArgumentException(
                    String.format("invalid unstable.slice %d > %d", lo, hi))
        }

        val upper = this.offset + entries.size
        if (lo < this.offset || hi > upper) {
            throw IllegalArgumentException(String
                    .format("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, this.offset, upper))
        }
    }

    companion object {

        val log = LoggerFactory.getLogger(Unstable::class.java)
    }
}
