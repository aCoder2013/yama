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
import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.utils.ErrorCode
import com.song.yama.raft.utils.ProtoBufUtils.buildEntry
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MemoryRaftStorageTest {

    @Test
    fun testStorageTerm() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())

        class Item(val i: Long, val werr: ErrorCode, val wterm: Long, val wpanic: Boolean)

        val tests = mutableListOf<Item>(
                Item(2, ErrorCode.ErrCompacted, 0, false),
                Item(3, ErrorCode.OK, 3, false),
                Item(4, ErrorCode.OK, 4, false),
                Item(5, ErrorCode.OK, 5, false),
                Item(6, ErrorCode.ErrUnavailable, 0, false))

        tests.forEachIndexed { i, tt ->
            try {
                val s = MemoryRaftStorage(ents)
                val result = s.term(tt.i)
                assertEquals(tt.werr, ErrorCode.getByCode(result.code))
                assertEquals(tt.wterm, result.data)
            } catch (e: Exception) {
                if (!tt.wpanic) {
                    throw RaftException(String.format("%d: panic = %b, want %b", i, true, tt.wpanic), e)
                }
            }
        }
    }

    @Test
    fun testStorageEntries() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(6).setTerm(6).build())

        class Item(val lo: Long, val hi: Long, val maxSize: Long, val weror: ErrorCode, val wentries: List<RaftProtoBuf.Entry>?)

        val tests = mutableListOf<Item>(
                Item(2, 6, Long.MAX_VALUE, ErrorCode.ErrCompacted, null),
                Item(3, 4, Long.MAX_VALUE, ErrorCode.ErrCompacted, null),
                Item(4, 5, Long.MAX_VALUE, ErrorCode.OK, mutableListOf(buildEntry(4, 4))),
                Item(4, 6, Long.MAX_VALUE, ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5))),
                Item(4, 7, Long.MAX_VALUE, ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5), buildEntry(6, 6))),
                // even if maxsize is zero, the first entry should be returned
                Item(4, 7, 0, ErrorCode.OK, mutableListOf(buildEntry(4, 4))),
                // limit to 2
                Item(4, 7, ents[1].toByteArray().size.toLong() + ents[2].toByteArray().size.toLong(), ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5))),
                // limit to 2
                Item(4, 7, ents[1].toByteArray().size.toLong() + ents[2].toByteArray().size.toLong() + ents[3].toByteArray().size.toLong() / 2, ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5))),
                Item(4, 7, ents[1].toByteArray().size.toLong() + ents[2].toByteArray().size.toLong() + ents[3].toByteArray().size.toLong() - 1, ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5))),
                // all
                Item(4, 7, ents[1].toByteArray().size.toLong() + ents[2].toByteArray().size.toLong() + ents[3].toByteArray().size.toLong(), ErrorCode.OK, mutableListOf(buildEntry(4, 4), buildEntry(5, 5), buildEntry(6, 6))))

        tests.forEachIndexed { i, tt ->
            val s = MemoryRaftStorage(ents)
            val result = s.entries(tt.lo, tt.hi, tt.maxSize)
            assertEquals(tt.weror, ErrorCode.getByCode(result.code))
            assertEquals(tt.wentries, result.data)
        }
    }

    @Test
    fun testStorageLastIndex() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())
        val s = MemoryRaftStorage(ents)
        val lastIndex = s.lastIndex()
        assertEquals(5, lastIndex)

        s.append(mutableListOf(buildEntry(6, 5)))
        val last = s.lastIndex()
        assertEquals(6, last)

    }

    @Test
    fun testStorageFirstIndex() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())
        val s = MemoryRaftStorage(ents)
        var first = s.firstIndex()
        assertEquals(4, first)

        s.compact(4)
        first = s.firstIndex()
        assertEquals(5, first)
    }

    @Test
    fun testStorageCompact() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())

        class Item(val i: Long, val werr: ErrorCode, val windex: Long, val wterm: Long, val wlen: Long)

        val tests = mutableListOf<Item>(
                Item(2, ErrorCode.ErrCompacted, 3, 3, 3),
                Item(3, ErrorCode.ErrCompacted, 3, 3, 3),
                Item(4, ErrorCode.OK, 4, 4, 2),
                Item(5, ErrorCode.OK, 5, 5, 1))

        tests.forEachIndexed { i, tt ->
            val s = MemoryRaftStorage(ents)
            val result = s.compact(tt.i)
            assertEquals(tt.werr, ErrorCode.getByCode(result.code))
            assertEquals(tt.windex, s.ents[0].index)
            assertEquals(tt.wterm, s.ents[0].term)
            assertEquals(tt.wlen, s.ents.size.toLong())
        }
    }

    @Test
    fun testStorageCreateSnapshot() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())

        val cs = RaftProtoBuf.ConfState.newBuilder().addAllNodes(mutableListOf(1, 2, 3)).build()

        val data = ByteString.copyFrom("data".toByteArray())

        class Item(val i: Long, val werr: ErrorCode, val wsnap: RaftProtoBuf.Snapshot)

        val tests = mutableListOf<Item>(
                Item(4, ErrorCode.OK, RaftProtoBuf.Snapshot.newBuilder().setData(data).setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(4).setConfState(cs)).build()),
                Item(5, ErrorCode.OK, RaftProtoBuf.Snapshot.newBuilder().setData(data).setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(5).setTerm(5).setConfState(cs)).build()))

        tests.forEachIndexed { i, tt ->
            val s = MemoryRaftStorage(ents)
            val result = s.createSnapshot(tt.i, cs, data.toByteArray())
            assertEquals(tt.werr, ErrorCode.getByCode(result.code))
            assertEquals(tt.wsnap, result.data)
        }
    }

    @Test
    fun testStorageAppend() {
        val ents = mutableListOf<RaftProtoBuf.Entry>(
                RaftProtoBuf.Entry.newBuilder().setIndex(3).setTerm(3).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(4).setTerm(4).build(),
                RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(5).build())

        class Item(val entries: List<RaftProtoBuf.Entry>, val werr: ErrorCode, val wentries: List<RaftProtoBuf.Entry>)

        val tests = mutableListOf<Item>(
                Item(mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 5)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 5))),
                Item(mutableListOf(buildEntry(3, 3), buildEntry(4, 6), buildEntry(5, 6)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 6), buildEntry(5, 6))),
                Item(mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 5), buildEntry(6, 5)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 5), buildEntry(6, 5))),
                // truncate incoming entries, truncate the existing entries and append
                Item(mutableListOf(buildEntry(2, 3), buildEntry(3, 3), buildEntry(4, 5)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 5))),
                // truncate the existing entries and append
                Item(mutableListOf(buildEntry(4, 5)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 5))),
                // direct append
                Item(mutableListOf(buildEntry(6, 5)), ErrorCode.OK, mutableListOf(buildEntry(3, 3), buildEntry(4, 4), buildEntry(5, 5), buildEntry(6, 5))))

        tests.forEachIndexed { i, tt ->
            val s = MemoryRaftStorage(ents)

            val result = s.append(tt.entries)
            assertEquals(tt.werr, ErrorCode.getByCode(result.code))
            assertEquals(tt.wentries, s.ents)
        }
    }

    @Test
    fun testStorageApplySnapshot() {
        val cs = RaftProtoBuf.ConfState.newBuilder().addAllNodes(mutableListOf(1, 2, 3)).build()
        val data = "data".toByteArray()

        val tests = mutableListOf<RaftProtoBuf.Snapshot>(
                RaftProtoBuf.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(4).setConfState(cs).build()).build(),
                RaftProtoBuf.Snapshot.newBuilder().setData(ByteString.copyFrom(data)).setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(3).setTerm(3).setConfState(cs).build()).build()
        )

        val s = MemoryRaftStorage()
        //Apply Snapshot successful
        var i = 0
        var tt = tests[i]
        var result = s.applySnapshot(tt)
        assertTrue { result.isSuccess }

        //Apply Snapshot fails due to ErrSnapOutOfDate
        i = 1
        tt = tests[i]
        result = s.applySnapshot(tt)
        assertEquals(ErrorCode.ErrSnapOutOfDate, ErrorCode.getByCode(result.code))
    }
}