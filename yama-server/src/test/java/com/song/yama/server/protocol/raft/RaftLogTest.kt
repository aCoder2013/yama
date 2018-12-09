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

package com.song.yama.server.protocol.raft

import com.song.yama.common.utils.Result
import com.song.yama.server.protocol.raft.exception.RaftException
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf
import com.song.yama.server.protocol.raft.utils.ErrorCode
import com.song.yama.server.protocol.raft.utils.ProtoBufUtils.buildEntry
import org.apache.commons.collections4.CollectionUtils
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RaftLogTest {

    @Test
    fun testFindConflict() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2),
                buildEntry(3, 3))

        class Item(val ents: List<RaftProtoBuf.Entry>, val wconflict: Long)

        val tests = mutableListOf<Item>(
                // no conflict, empty ent
                Item(mutableListOf(), 0),
                // no conflict
                Item(mutableListOf(buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 3)), 0),
                Item(mutableListOf(buildEntry(2, 2), buildEntry(3, 3)), 0),
                Item(mutableListOf(buildEntry(3, 3)), 0),
                // no conflict, but has new entries
                Item(mutableListOf(buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 4)), 4),
                Item(mutableListOf(buildEntry(2, 2), buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 4)), 4),
                Item(mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 4)), 4),
                Item(mutableListOf(buildEntry(4, 4), buildEntry(5, 4)), 4),
                // conflicts with existing entries
                Item(mutableListOf(buildEntry(1, 4), buildEntry(2, 4)), 1),
                Item(mutableListOf(buildEntry(2, 1), buildEntry(3, 4), buildEntry(4, 4)), 2),
                Item(mutableListOf(buildEntry(3, 1), buildEntry(4, 2), buildEntry(5, 4), buildEntry(6, 4)), 3))

        tests.forEachIndexed { i, tt ->
            val raftLog = RaftLog(MemoryRaftStorage(), Long.MAX_VALUE)
            raftLog.append(previousEnts)

            val gconflict = raftLog.findConflict(tt.ents)
            assertEquals(tt.wconflict, gconflict)
        }
    }

    @Test
    fun testIsUpToDate() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2),
                buildEntry(3, 3))

        val raftLog = RaftLog(MemoryRaftStorage())
        raftLog.append(previousEnts)

        class Item(val lastIndex: Long, val term: Long, val wUpToDate: Boolean)

        val tests = mutableListOf<Item>(
                // greater term, ignore lastIndex
                Item(raftLog.lastIndex() - 1, 4, true),
                Item(raftLog.lastIndex(), 4, true),
                Item(raftLog.lastIndex() + 1, 4, true),
                // smaller term, ignore lastIndex
                Item(raftLog.lastIndex() - 1, 2, false),
                Item(raftLog.lastIndex(), 2, false),
                Item(raftLog.lastIndex() + 1, 2, false),
                // equal term, equal or lager lastIndex wins
                Item(raftLog.lastIndex() - 1, 3, false),
                Item(raftLog.lastIndex(), 3, true),
                Item(raftLog.lastIndex() + 1, 3, true))

        tests.forEachIndexed { i, tt ->
            val gUpToDate = raftLog.isUpToDate(tt.lastIndex, tt.term)
            assertEquals(tt.wUpToDate, gUpToDate)
        }
    }


    @Test
    fun testAppend() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2))

        class Item(val ents: List<RaftProtoBuf.Entry>, val windex: Long, val wents: List<RaftProtoBuf.Entry>, val wunstable: Long)

        val tests = mutableListOf(
                Item(mutableListOf(), 2, mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 3),
                Item(mutableListOf(buildEntry(3, 2)), 3, mutableListOf(buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 2)), 3),
                // conflicts with index 1
                Item(mutableListOf(buildEntry(1, 2)), 1, mutableListOf(buildEntry(1, 2)), 1),
                // conflicts with index 2
                Item(mutableListOf(buildEntry(2, 3), buildEntry(3, 3)), 3, mutableListOf(buildEntry(1, 1), buildEntry(2, 3), buildEntry(3, 3)), 2))
        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.append(previousEnts)
            val raftLog = RaftLog(storage)

            val index = raftLog.append(tt.ents)
            assertEquals(tt.windex, index, "$i ,index should be equal")

            val result = raftLog.entries(1, Long.MAX_VALUE)
            assertEquals(ErrorCode.OK, ErrorCode.getByCode(result.code))
            assertEquals(tt.wents, result.data)
            val goff = raftLog.unstable.offset
            assertEquals(tt.wunstable, goff)
        }
    }

    /**
     * testLogMaybeAppend ensures:
     * If the given (index, term) matches with the existing log:
     * 1. If an existing entry conflicts with a new one (same index
     * but different terms), delete the existing entry and all that
     * follow it
     * 2.Append any new entries not already in the log
     * If the given (index, term) does not match with the existing log:
     * return false
     */
    @Test
    fun testLogMaybeAppend() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2),
                buildEntry(3, 3))
        val lastindex = 3L
        val lastterm = 3L
        val commit = 1L

        class Item(val logTerm: Long, val index: Long, val committed: Long, val ents: List<RaftProtoBuf.Entry>?, val wlasti: Long, val wappend: Boolean, val wcommit: Long, val wpanic: Boolean)

        val tests = mutableListOf<Item>(
                // not match: term is different
                Item(lastterm - 1, lastindex, lastindex, mutableListOf(buildEntry(lastindex + 1, 4)), 0, false, commit, false),
                // not match: index out of bound
                Item(lastterm, lastindex + 1, lastindex, mutableListOf(buildEntry(lastindex + 2, 4)), 0, false, commit, false),
                // match with the last existing entry
                Item(lastterm, lastindex, lastindex, null, lastindex, true, lastindex, false),
                Item(lastterm, lastindex, lastindex + 1, null, lastindex, true, lastindex, false), // do not increase commit higher than lastnewi
                Item(lastterm, lastindex, lastindex - 1, null, lastindex, true, lastindex - 1, false),// commit up to the commit in the message
                Item(lastterm, lastindex, 0, null, lastindex, true, commit, false), // commit do not decrease
                Item(0, 0, lastindex, null, 0, true, commit, false),  // commit do not decrease
                Item(lastterm, lastindex, lastindex, mutableListOf(buildEntry(lastindex + 1, 4)), lastindex + 1, true, lastindex, false),
                Item(lastterm, lastindex, lastindex + 1, mutableListOf(buildEntry(lastindex + 1, 4)), lastindex + 1, true, lastindex + 1, false),
                Item(lastterm, lastindex, lastindex + 2, mutableListOf(buildEntry(lastindex + 1, 4)), lastindex + 1, true, lastindex + 1, false),// do not increase commit higher than lastnewi
                Item(lastterm, lastindex, lastindex + 2, mutableListOf(buildEntry(lastindex + 1, 4), buildEntry(lastindex + 2, 4)), lastindex + 2, true, lastindex + 2, false),
                // match with the the entry in the middle
                Item(lastterm - 1, lastindex - 1, lastindex, mutableListOf(buildEntry(lastindex, 4)), lastindex, true, lastindex, false),
                Item(lastterm - 2, lastindex - 2, lastindex, mutableListOf(buildEntry(lastindex - 1, 4)), lastindex - 1, true, lastindex - 1, false),
                Item(lastterm - 3, lastindex - 3, lastindex, mutableListOf(buildEntry(lastindex - 2, 4)), lastindex - 2, true, lastindex - 2, true),  // conflict with existing committed entry
                Item(lastterm - 2, lastindex - 2, lastindex, mutableListOf(buildEntry(lastindex - 1, 4), buildEntry(lastindex, 4)), lastindex, true, lastindex, false))

        tests.forEachIndexed { i, tt ->
            val raftLog = RaftLog(MemoryRaftStorage())
            raftLog.append(previousEnts)
            raftLog.committed = commit
            try {
                val result = raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents)
                val gcommit = raftLog.committed

                assertEquals(tt.wlasti, result.data)
                assertEquals(tt.wappend, result.isSuccess)
                assertEquals(tt.wcommit, gcommit)

                if (result.isSuccess && CollectionUtils.size(tt.ents) != 0) {
                    val sliceResult = raftLog.slice(raftLog.lastIndex() - tt.ents!!.size + 1, raftLog.lastIndex() + 1, Long.MAX_VALUE)
                    assertTrue(sliceResult.isSuccess)
                    assertEquals(tt.ents, sliceResult.data)
                }
            } catch (e: Exception) {
                if (!tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %b, want %b", i, true, tt.wpanic), e)
                }
            }
        }
    }

    /**
     * TestCompactionSideEffects ensures that all the log related functionality works correctly after
     * a compaction.
     */
    @Test
    fun testCompactionSideEffects() {
        // Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
        var lastIndex = 1000L
        var unstableIndex = 750L
        var lastTerm = lastIndex
        val storage = MemoryRaftStorage()
        for (i in 1..unstableIndex) {
            storage.append(mutableListOf(RaftProtoBuf.Entry.newBuilder().setTerm(i).setIndex(i).build()))
        }
        val raftLog = RaftLog(storage)
        for (i in unstableIndex until lastIndex) {
            raftLog.append(mutableListOf(RaftProtoBuf.Entry.newBuilder().setTerm(i + 1).setIndex(i + 1).build()))
        }

        val ok = raftLog.maybeCommit(lastIndex, lastTerm)
        assertTrue(ok)
        raftLog.appliedTo(raftLog.committed)

        val offset = 500L
        storage.compact(offset)

        assertEquals(lastIndex, raftLog.lastIndex())
        for (i in offset..raftLog.lastIndex()) {
            assertEquals(i, mustTerm(raftLog.term(i)))
        }

        for (i in offset..raftLog.lastIndex()) {
            assertTrue(raftLog.matchTerm(i, i))
        }

        val unstableEntries = raftLog.unstableEntries()
        assertEquals(250, CollectionUtils.size(unstableEntries))
        assertEquals(751, unstableEntries[0].index)

        val pre = raftLog.lastIndex()
        raftLog.append(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(raftLog.lastIndex() + 1).setTerm(raftLog.lastIndex() + 1).build()))
        assertEquals(pre + 1, raftLog.lastIndex())
        val result = raftLog.entries(raftLog.lastIndex(), Long.MAX_VALUE)
        assertTrue(result.isSuccess)
        assertEquals(1, CollectionUtils.size(result.data))
    }

    @Test
    fun testHasNextEnts() {
        val snap = RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3).build()).build()
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(4, 1),
                buildEntry(5, 1),
                buildEntry(6, 1))

        class Item(val applied: Long, var hasNext: Boolean)

        val tests = mutableListOf<Item>(
                Item(0, true),
                Item(3, true),
                Item(4, true),
                Item(5, false))

        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.applySnapshot(snap)

            val raftLog = RaftLog(storage)
            raftLog.append(ents)
            raftLog.maybeCommit(5, 1)
            raftLog.appliedTo(tt.applied)

            val hasNext = raftLog.hasNextEnts()
            assertEquals(tt.hasNext, hasNext)
        }
    }

    @Test
    fun testNextEnts() {
        val snap = RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setTerm(1).setIndex(3).build()).build()
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(4, 1),
                buildEntry(5, 1),
                buildEntry(6, 1))

        class Item(val applied: Long, val wents: List<RaftProtoBuf.Entry>?)

        val tests = mutableListOf<Item>(
                Item(0, ArrayList<RaftProtoBuf.Entry>(ents.subList(0, 2))),
                Item(3, ArrayList<RaftProtoBuf.Entry>(ents.subList(0, 2))),
                Item(4, ArrayList<RaftProtoBuf.Entry>(ents.subList(1, 2))),
                Item(5, emptyList()))

        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.applySnapshot(snap)
            val raftLog = RaftLog(storage)
            raftLog.append(ents)
            raftLog.maybeCommit(5, 1)
            raftLog.appliedTo(tt.applied)

            val nents = raftLog.nextEnts()
            assertEquals(tt.wents, nents)
        }
    }

    /**
     * testUnstableEnts ensures unstableEntries returns the unstable part of the
     * entries correctly.
     */
    @Test
    fun testUnstableEnts() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2))

        class Item(val unstable: Long, val wents: List<RaftProtoBuf.Entry>?)

        val tests = mutableListOf<Item>(
                Item(3, emptyList()),
                Item(1, previousEnts))
        tests.forEachIndexed { i, tt ->
            // append stable entries to storage
            val storage = MemoryRaftStorage()
            storage.append(ArrayList<RaftProtoBuf.Entry>(previousEnts.subList(0, (tt.unstable - 1).toInt())))

            // append unstable entries to raftlog
            val raftLog = RaftLog(storage)
            raftLog.append(ArrayList<RaftProtoBuf.Entry>(previousEnts.subList((tt.unstable - 1).toInt(), previousEnts.size)))

            val ents = raftLog.unstableEntries()
            if (CollectionUtils.isNotEmpty(ents)) {
                val size = ents.size
                raftLog.stableTo(ents[size - 1].index, ents[size - i].term)
            }

            assertEquals(tt.wents, ents)
            val w = previousEnts[previousEnts.size - 1].index + 1
            val g = raftLog.unstable.offset
            assertEquals(w, g)
        }
    }

    @Test
    fun testCommitTo() {
        val previousEnts = mutableListOf<RaftProtoBuf.Entry>(
                buildEntry(1, 1),
                buildEntry(2, 2),
                buildEntry(3, 3))
        val commit = 2L

        class Item(val commit: Long, val wcommit: Long, val wpanic: Boolean)

        val tests = mutableListOf<Item>(
                Item(3, 3, false),
                Item(1, 2, false),
                Item(4, 0, true))

        tests.forEachIndexed { i, tt ->
            try {
                val raftLog = RaftLog(MemoryRaftStorage())
                raftLog.append(previousEnts)
                raftLog.committed = commit
                raftLog.commitTo(tt.commit)
                assertEquals(tt.wcommit, raftLog.committed)
            } catch (e: Exception) {
                if (!tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %v, want %v", i, true, tt.wpanic), e)
                }
            }
        }
    }

    @Test
    fun testStableTo() {
        class Item(val stablei: Long, val stablet: Long, val wunstable: Long)

        val tests = mutableListOf<Item>(
                Item(1, 1, 2),
                Item(2, 2, 3),
                Item(2, 1, 1),// bad term
                Item(3, 1, 1))// bad index)
        tests.forEachIndexed { i, tt ->
            val raftLog = RaftLog(MemoryRaftStorage())
            raftLog.append(mutableListOf(buildEntry(1, 1), buildEntry(2, 2)))
            raftLog.stableTo(tt.stablei, tt.stablet)
            assertEquals(tt.wunstable, raftLog.unstable.offset)
        }
    }

    @Test
    fun testStableToWithSnap() {
        val snapi = 5L
        val snapt = 2L

        class Item(val stablei: Long, val stablet: Long, val newEnts: List<RaftProtoBuf.Entry>, val wunstable: Long)

        val tests = mutableListOf<Item>(
                Item(snapi + 1, snapt, emptyList(), snapi + 1),
                Item(snapi, snapt, emptyList(), snapi + 1),
                Item(snapi - 1, snapt, emptyList(), snapi + 1),
                Item(snapi + 1, snapt + 1, emptyList(), snapi + 1),
                Item(snapi, snapt + 1, emptyList(), snapi + 1),
                Item(snapi - 1, snapt + 1, emptyList(), snapi + 1),

                Item(snapi + 1, snapt, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 2),
                Item(snapi, snapt, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 1),
                Item(snapi - 1, snapt, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 1),
                Item(snapi + 1, snapt + 1, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 1),
                Item(snapi, snapt + 1, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 1),
                Item(snapi - 1, snapt + 1, mutableListOf(buildEntry(snapi + 1, snapt)), snapi + 1))

        tests.forEachIndexed { i, tt ->
            val s = MemoryRaftStorage()
            s.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(snapi).setTerm(snapt).build()).build())
            val raftLog = RaftLog(s)
            raftLog.run {
                append(tt.newEnts)
                stableTo(tt.stablei, tt.stablet)
                assertEquals(tt.wunstable, unstable.offset)
            }
        }
    }

    /**
     * testCompaction ensures that the number of log entries is correct after compactions.
     */
    @Test
    fun testCompaction() {
        class Item(val lastIndex: Long, val compact: List<Long>, val wleft: List<Int>, val wallow: Boolean)

        val tests = mutableListOf<Item>(
                Item(1000, mutableListOf(1001), mutableListOf(-1), false),
                Item(1000, mutableListOf(300, 500, 800, 900), mutableListOf(700, 500, 200, 100), true),
                // out of lower bound
                Item(1000, mutableListOf(300, 299), mutableListOf(700, -1), false))

        tests.forEachIndexed { i, tt ->
            try {
                val storage = MemoryRaftStorage()
                for (j in 1..tt.lastIndex) {
                    storage.append(mutableListOf(buildEntry(j)))
                }
                val raftLog = RaftLog(storage)
                raftLog.run {
                    maybeCommit(tt.lastIndex, 0)
                    appliedTo(raftLog.committed)
                }

                for (j in 0 until CollectionUtils.size(tt.compact)) {
                    val result = storage.compact(tt.compact[j])
                    if (result.isFailure) {
                        if (tt.wallow) {
                            throw RaftException(String.format("#%d.%d allow = %t, want %t", i, j, false, tt.wallow))
                        }
                        continue
                    }
                    assertEquals(tt.wleft[j], CollectionUtils.size(raftLog.allEntries()))
                }
            } catch (e: Exception) {
                if (tt.wallow) {
                    throw RaftException(String.format("%d: allow = %b, want %b:", i, false, true), e)
                }
            }
        }
    }

    @Test
    fun testLogRestore() {
        val index = 1000L
        val term = 1000L
        val snap = RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(index).setTerm(term).build()
        val storage = MemoryRaftStorage()
        storage.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(snap).build())
        val raftLog = RaftLog(storage)

        assertEquals(0, raftLog.allEntries().size)
        assertEquals(index + 1, raftLog.firstIndex())
        assertEquals(index, raftLog.committed)
        assertEquals(index + 1, raftLog.unstable.offset)
        assertEquals(term, mustTerm(raftLog.term(index)))
    }

    @Test
    fun testIsOutOfBounds() {
        val offset = 100L
        val num = 100
        val storage = MemoryRaftStorage()
        storage.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(offset).build()).build())
        val l = RaftLog(storage)
        for (i in 1..num) {
            l.append(mutableListOf(buildEntry(i + offset)))
        }

        val first = offset + 1

        class Item(val lo: Long, val hi: Long, val wpanic: Boolean, val wErrCompacted: Boolean)

        val tests = mutableListOf(
                Item(
                        first - 2, first + 1,
                        false,
                        true
                ),
                Item(
                        first - 1, first + 1,
                        false,
                        true
                ),
                Item(
                        first, first,
                        false,
                        false
                ),
                Item(
                        first + num / 2, first + num / 2,
                        false,
                        false
                ),
                Item(
                        first + num - 1, first + num - 1,
                        false,
                        false
                ),
                Item(
                        first + num, first + num,
                        false,
                        false
                ),
                Item(
                        first + num, first + num + 1,
                        true,
                        false
                ),
                Item(
                        first + num + 1, first + num + 1,
                        true,
                        false
                ))

        tests.forEachIndexed { i, tt ->
            try {
                val errorCode = l.mustCheckOutOfBounds(tt.lo, tt.hi)
                if (tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %v, want %v", i, false, true))
                }
                if (tt.wErrCompacted && errorCode != ErrorCode.ErrCompacted) {
                    throw RaftException("$i: err = $errorCode, want ${ErrorCode.ErrCompacted}")
                }
                if (!tt.wErrCompacted && errorCode != null && errorCode != ErrorCode.OK) {
                    throw RaftException("$i :unexpected err:$errorCode")
                }
            } catch (e: Exception) {
                if (!tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %b, want %b: ", i, true, false), e)
                }
            }
        }
    }

    @Test
    fun testTerm() {
        val offset = 100L
        val num = 100L
        val storage = MemoryRaftStorage()
        storage.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(offset).setTerm(1).build()).build())
        val l = RaftLog(storage)

        for (i in 1 until num) {
            l.append(mutableListOf(buildEntry(offset + i, i)))
        }

        class Item(val index: Long, val w: Long)

        val tests = mutableListOf<Item>(
                Item(offset - 1, 0),
                Item(offset, 1),
                Item(offset + num / 2, num / 2),
                Item(offset + num - 1, num - 1),
                Item(offset + num, 0))
        tests.forEachIndexed { i, tt ->
            val term = mustTerm(l.term(tt.index))
            assertEquals(tt.w, term)
        }
    }

    @Test
    fun testTermWithUnstableSnapshot() {
        val storagesnapi = 100L
        val unstablesnapi = storagesnapi + 5

        val storage = MemoryRaftStorage()
        storage.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(storagesnapi).setTerm(1).build()).build())
        val l = RaftLog(storage)
        l.restore(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(unstablesnapi).setTerm(1).build()).build())

        class Item(val index: Long, val w: Long)

        val tests = mutableListOf<Item>(
                // cannot get term from storage
                Item(storagesnapi, 0),
                // cannot get term from the gap between storage ents and unstable snapshot
                Item(storagesnapi + 1, 0),
                Item(unstablesnapi - 1, 0),
                // get term from unstable snapshot index
                Item(unstablesnapi, 1)
        )

        tests.forEachIndexed { i, tt ->
            val term = mustTerm(l.term(tt.index))
            assertEquals(tt.w, term)
        }
    }

    @Test
    fun testSlice() {
        val offset = 100L
        val num = 100L
        val last = offset + num
        val half = offset + num / 2
        val halfe = buildEntry(half, half)

        val storage = MemoryRaftStorage()
        storage.applySnapshot(RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(offset).build()).build())
        for (i in 1 until num / 2) {
            storage.append(mutableListOf(buildEntry(offset + i, offset + i)))
        }

        val l = RaftLog(storage)
        for (i in num / 2 until num) {
            l.append(mutableListOf(buildEntry(offset + i, offset + i)))
        }

        class Item(val from: Long, val to: Long, val limit: Long, val w: List<RaftProtoBuf.Entry>, val wpanic: Boolean)

        val tests = mutableListOf<Item>(
                // test no limit
                Item(offset - 1, offset + 1, Long.MAX_VALUE, emptyList(), false),
                Item(offset, offset + 1, Long.MAX_VALUE, emptyList(), false),
                Item(half - 1, half + 1, Long.MAX_VALUE, mutableListOf<RaftProtoBuf.Entry>(buildEntry(half - 1, half - 1), buildEntry(half, half)), false),
                Item(half, half + 1, Long.MAX_VALUE, mutableListOf<RaftProtoBuf.Entry>(buildEntry(half, half)), false),
                Item(last - 1, last, Long.MAX_VALUE, mutableListOf<RaftProtoBuf.Entry>(buildEntry(last - 1, last - 1)), false),
                Item(last, last + 1, Long.MAX_VALUE, emptyList(), true),

                // test limit
                Item(half - 1, half + 1, 0, mutableListOf(buildEntry(half - 1, half - 1)), false),
                Item(half - 1, half + 1, (halfe.toByteArray().size + 1).toLong(), mutableListOf(buildEntry(half - 1, half - 1)), false),
                Item(half - 2, half + 1, (halfe.toByteArray().size + 1).toLong(), mutableListOf(buildEntry(half - 2, half - 2)), false),
                Item(half - 1, half + 1, (halfe.toByteArray().size * 2).toLong(), mutableListOf(buildEntry(half - 1, half - 1), buildEntry(half, half)), false),
                Item(half - 1, half + 2, (halfe.toByteArray().size * 3).toLong(), mutableListOf(buildEntry(half - 1, half - 1), buildEntry(half, half), buildEntry(half + 1, half + 1)), false),
                Item(half, half + 2, (halfe.toByteArray().size).toLong(), mutableListOf(buildEntry(half, half)), false),
                Item(half, half + 2, (halfe.toByteArray().size * 2).toLong(), mutableListOf(buildEntry(half, half), buildEntry(half + 1, half + 1)), false))

        tests.forEachIndexed { i, tt ->
            try {
                val result = l.slice(tt.from, tt.to, tt.limit)
                if (tt.from <= offset && result.code != ErrorCode.ErrCompacted.code) {
                    throw RaftException("#$i: err = ${ErrorCode.getByCode(result.code)}, want ${ErrorCode.ErrCompacted}")
                }

                if (tt.from > offset && result.isFailure) {
                    throw RaftException("#$i unexpected error: ${ErrorCode.getByCode(result.code)}")
                }

                assertEquals(tt.w, result.data, "#$i : w should be equal")
            } catch (e: Exception) {
                if (!tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %v, want %v: ", i, true, false), e)
                }
            }
        }
    }

    private fun mustTerm(result: Result<Long>): Long {
        if (result.isFailure) {
            throw RaftException(ErrorCode.getByCode(result.code).desc)
        }

        return result.data
    }
}