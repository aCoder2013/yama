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
import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot
import com.song.yama.raft.utils.ErrorCode
import com.song.yama.raft.utils.Utils
import org.apache.commons.collections4.CollectionUtils
import org.slf4j.LoggerFactory

class RaftLog @JvmOverloads constructor(
        /**
         * storage contains all stable entries since the last snapshot
         */
        val raftStorage: RaftStorage, var maxMsgSize: Long = java.lang.Long.MAX_VALUE) {

    /**
     * unstable contains all unstable entries and snapshot. they will be saved into storage
     */
    var unstable: Unstable

    /**
     * committed is the highest log position that is known to be in stable storage on a quorum of nodes.
     */
    var committed: Long = 0

    /**
     * applied is the highest log position that the application has been instructed to apply to its state machine.
     * Invariant: applied <= committed
     */
    var applied: Long = 0

    init {
        val firstIndex = raftStorage.firstIndex()
        val lastIndex = raftStorage.lastIndex()

        this.unstable = Unstable()
        this.unstable.offset = lastIndex + 1
        this.committed = firstIndex - 1
        this.applied = firstIndex - 1
    }

    /**
     * maybeAppend returns (0, false) if the entries cannot be appended. Otherwise, it returns (last index of new
     * entries, true).
     */
    @Synchronized
    fun maybeAppend(index: Long, logTerm: Long, committed: Long,
                    ents: List<RaftProtoBuf.Entry>): Result<Long> {
        if (matchTerm(index, logTerm)) {
            val lastnewi = index + CollectionUtils.size(ents)
            val ci = findConflict(ents)
            if (ci == 0L) {

            } else if (ci <= this.committed) {
                val message = String
                        .format("entry %d conflict with committed entry [committed(%d)]", ci, this.committed)
                log.warn(message)
                throw RaftException(message)
            } else {
                val offset = index + 1
                append(ents.subList((ci - offset).toInt(), ents.size).toMutableList())
            }
            commitTo(Math.min(committed, lastnewi))
            return Result.success(lastnewi)
        }
        return Result.fail(0L)
    }

    /**
     * / findConflict finds the index of the conflict. It returns the first pair of conflicting entries between the
     * existing entries and the given entries, if there are any. If there is no conflicting entries, and the existing
     * entries contains all the given entries, zero will be returned. If there is no conflicting entries, but the given
     * entries contains new entries, the index of the first new entry will be returned. An entry is considered to be
     * conflicting if it has the same index but a different term. The first entry MUST have an index equal to the
     * argument 'from'. The index of the given entries MUST be continuously increasing.
     */
    @Synchronized
    fun findConflict(ents: List<RaftProtoBuf.Entry>?): Long {
        if (ents != null && ents.size > 0) {
            for (entry in ents) {
                if (!matchTerm(entry.index, entry.term)) {
                    if (entry.index <= lastIndex()) {
                        log.info(String.format(
                                "found conflict at index %d [existing term: %d, conflicting term: %d]",
                                entry.index,
                                this.zeroTermOnErrCompacted(this.term(entry.index).data, null),
                                entry.term))
                    }
                    return entry.index
                }
            }
        }
        return 0
    }

    @Synchronized
    fun unstableEntries(): List<RaftProtoBuf.Entry> {
        return if (CollectionUtils.isEmpty(this.unstable!!.entries)) {
            emptyList()
        } else this.unstable!!.entries

    }

    /**
     * nextEnts returns all the available entries for execution. If applied is smaller than the index of snapshot, it
     * returns all committed entries after the index of snapshot.
     */
    @Synchronized
    fun nextEnts(): List<Entry> {
        val off = Math.max(this.applied + 1, firstIndex())
        if (this.committed + 1 > off) {
            val result = slice(off, this.committed + 1, maxMsgSize)
            if (result.isFailure) {
                throw IllegalStateException(
                        String.format("unexpected error when getting unapplied entries (%s)",
                                ErrorCode.getByCode(result.code).desc))
            }
            return result.data
        }
        return emptyList()
    }


    /**
     * hasNextEnts returns if there is any available entries for execution. This is a fast check without heavy
     * raftLog.slice() in raftLog.nextEnts().
     */
    @Synchronized
    fun hasNextEnts(): Boolean {
        val off = Math.max(this.applied + 1, firstIndex())
        return this.committed + 1 > off
    }

    @Synchronized
    fun snapshot(): Result<Snapshot> {
        return if (this.unstable!!.snapshot != null) {
            Result.success(this.unstable!!.snapshot)
        } else this.raftStorage!!.snapshot()
    }

    @Synchronized
    fun commitTo(toCommit: Long) {
        // never decrease commit
        if (this.committed < toCommit) {
            if (lastIndex() < toCommit) {
                throw IllegalStateException(String.format(
                        "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
                        toCommit, lastIndex()))
            }

            this.committed = toCommit
        }
    }

    @Synchronized
    fun appliedTo(i: Long) {
        if (i == 0L) {
            return
        }
        if (committed < i || i < applied) {
            throw IllegalStateException(String
                    .format("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i,
                            this.applied, this.committed))
        }

        applied = i
    }


    @Synchronized
    fun stableTo(i: Long, t: Long) {
        this.unstable!!.stableTo(i, t)
    }

    @Synchronized
    fun stableSnapTo(i: Long) {
        this.unstable!!.stableSnapTo(i)
    }

    @Synchronized
    fun lastTerm(): Long {
        val result = term(lastIndex())
        if (result.isSuccess) {
            return result.data
        }
        throw RaftException("Unexpected error when getting the last term : " + result.code)
    }

    @Synchronized
    fun append(entries: MutableList<Entry>): Long {
        if (CollectionUtils.isEmpty(entries)) {
            return lastIndex()
        }

        val after = entries[0].index - 1
        if (after < committed) {
            throw IllegalStateException(
                    String.format("after(%d) is out of range [committed(%d)]", after, this.committed))
        }

        this.unstable.truncateAndAppend(entries)
        return lastIndex()
    }

    @Synchronized
    fun firstIndex(): Long {
        val result = this.unstable!!.maybeFirstIndex()
        return if (result.isSuccess) {
            result.data
        } else this.raftStorage!!.firstIndex()

    }

    @Synchronized
    fun lastIndex(): Long {
        val result = unstable!!.maybeLastIndex()
        return if (result.isSuccess) {
            result.data
        } else raftStorage.lastIndex()
    }

    @Synchronized
    fun term(i: Long): Result<Long> {
        // the valid term range is [index of dummy entry, last index]
        val dummyIndex = firstIndex() - 1
        if (i < dummyIndex || i > lastIndex()) {
            return Result.success(0L)
        }

        val result = this.unstable!!.maybeTerm(i)
        if (result.isSuccess) {
            return result
        }

        val termResult = this.raftStorage!!.term(i)
        if (termResult.isSuccess) {
            return termResult
        }

        if (result.code == ErrorCode.ErrCompacted.code || result.code == ErrorCode.ErrUnavailable.code) {
            return Result.success(0L)
        }

        throw IllegalArgumentException("Unexpected error" + ErrorCode.getByCode(result.code).desc)
    }

    @Synchronized
    fun entries(i: Long, maxSize: Long): Result<List<Entry>> {
        return if (i > lastIndex()) {
            Result.success(emptyList())
        } else slice(i, lastIndex() + 1, maxSize)

    }

    @Synchronized
    fun allEntries(): List<Entry> {
        val result = entries(firstIndex(), java.lang.Long.MAX_VALUE)
        if (result.isSuccess) {
            return result.data
        }

        if (result.code == ErrorCode.ErrCompacted
                        .code) {
            //TODO:try again if there was a racing compaction
            return allEntries()
        }

        //FIXME:handle error
        throw IllegalStateException(ErrorCode.getByCode(result.code).desc)
    }

    /**
     * isUpToDate determines if the given (lastIndex,term) log is more up-to-date by comparing the index and term of the
     * last entries in the existing logs. If the logs have last entries with different terms, then the log with the
     * later term is more up-to-date. If the logs end with the same term, then whichever log has the larger lastIndex is
     * more up-to-date. If the logs are the same, the given log is up-to-date.
     */
    @Synchronized
    fun isUpToDate(lasti: Long, term: Long): Boolean {
        return term > lastTerm() || term == lastTerm() && lasti >= lastIndex()
    }

    @Synchronized
    fun matchTerm(i: Long, term: Long): Boolean {
        try {
            val result = term(i)
            return result.isSuccess && result.data == term
        } catch (e: Exception) {
            log.warn(e.message, e)
            return false
        }

    }


    @Synchronized
    fun maybeCommit(maxIndex: Long, term: Long): Boolean {
        if (maxIndex > this.committed && zeroTermOnErrCompacted(term(maxIndex).data, null) == term) {
            commitTo(maxIndex)
            return true
        }
        return false
    }

    @Synchronized
    fun restore(snapshot: RaftProtoBuf.Snapshot) {
        log.info(String.format("log [%s] starts to restore snapshot [index: %d, term: %d]", this,
                snapshot.metadata.index, snapshot.metadata.term))
        this.committed = snapshot.metadata.index
        this.unstable!!.restore(snapshot)
    }

    @Synchronized
    fun slice(lo: Long, hi: Long, maxSize: Long): Result<List<Entry>> {
        val errorCode = mustCheckOutOfBounds(lo, hi)
        if (errorCode != null) {
            return Result.fail(errorCode.code, errorCode.desc, emptyList())
        }

        if (lo == hi) {
            return Result.success(emptyList())
        }

        var ents: MutableList<Entry>? = null
        if (lo < this.unstable.offset) {
            val result = raftStorage
                    .entries(lo, Math.min(hi, this.unstable.offset), maxSize)
            if (result.isFailure) {
                val code = ErrorCode.getByCode(result.code)
                if (code === ErrorCode.ErrCompacted) {
                    return Result.fail(code.code, code.desc, emptyList())
                } else if (code === ErrorCode.ErrUnavailable) {
                    throw IllegalStateException(String
                            .format("entries[%d:%d) is unavailable from storage", lo,
                                    Math.min(hi, this.unstable.offset)))
                } else {
                    throw IllegalStateException(code.desc)
                }
            }

            if (result.data.size < Math.min(hi, this.unstable!!.offset) - lo) {
                return Result.success(result.data)
            }

            ents = result.data.toMutableList()
        }

        if (hi > this.unstable!!.offset) {
            val entries = this.unstable!!.slice(Math.max(lo, this.unstable!!.offset), hi)
            if (CollectionUtils.isNotEmpty(ents)) {
                ents!!.addAll(entries)
            } else {
                ents = entries
            }
        }

        return Result.success(Utils.limitSize(ents!!, maxSize))
    }

    // l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
    @Synchronized
    fun mustCheckOutOfBounds(lo: Long, hi: Long): ErrorCode? {
        if (lo > hi) {
            throw IllegalStateException(String.format("invalid slice %d > %d", lo, hi))
        }

        val fi = firstIndex()

        if (lo < fi) {
            return ErrorCode.ErrCompacted
        }

        val length = lastIndex() + 1 - fi
        if (lo < fi || hi > fi + length) {
            throw IllegalStateException(
                    String.format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()))
        }

        return null
    }

    @Synchronized
    fun zeroTermOnErrCompacted(t: Long?, errorCode: ErrorCode?): Long {
        var t = t
        if (t == null) {
            log.debug("t is null , change it into zero")
            t = 0L
        }
        if (errorCode == null) {
            return t
        }

        if (errorCode === ErrorCode.ErrCompacted) {
            return 0
        }

        throw IllegalStateException(
                String.format("unexpected error (%d)", errorCode.code))
    }

    companion object {

        private val log = LoggerFactory.getLogger(RaftLog::class.java)
    }
}
