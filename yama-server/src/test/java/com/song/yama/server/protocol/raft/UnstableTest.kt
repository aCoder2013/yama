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

import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf
import org.apache.commons.collections4.CollectionUtils
import org.junit.Test
import kotlin.test.assertEquals

class UnstableTest {

    @Test
    fun testUnstableMaybeFirstIndex() {
        class Item(val entries: List<RaftProtoBuf.Entry>, val offset: Long, val snap: RaftProtoBuf.Snapshot?, val wok: Boolean, val windex: Long)

        val tests = mutableListOf(
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, false, 0),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().build()), 0, null, false, 0),
                // has snapshot
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), true, 5),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), true, 5))
        tests.forEachIndexed { i, tt ->
            val u = Unstable()
            u.entries = tt.entries
            u.offset = tt.offset
            u.snapshot = tt.snap
            val result = u.maybeFirstIndex()
            assertEquals(result.isSuccess, tt.wok)
            assertEquals(result.data, tt.windex, "index:$i is not equal")
        }
    }

    @Test
    fun testMaybeLastIndex() {
        class Item(val entries: List<RaftProtoBuf.Entry>, val offset: Long, val snap: RaftProtoBuf.Snapshot?, val wok: Boolean, val windex: Long)

        val tests = mutableListOf(
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, true, 5),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), true, 5),
                Item(mutableListOf(), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), true, 4),
                Item(mutableListOf(), 0, null, false, 0))
        tests.forEachIndexed { i, tt ->
            val u = Unstable()
            u.entries = tt.entries
            u.offset = tt.offset
            u.snapshot = tt.snap

            val result = u.maybeLastIndex()
            assertEquals(tt.wok, result.isSuccess)
            assertEquals(tt.windex, result.data, "index:$i is not equal")
        }
    }

    @Test
    fun testUnstableMaybeTerm() {
        class Item(val entries: List<RaftProtoBuf.Entry>, val offset: Long, val snap: RaftProtoBuf.Snapshot?, val index: Long, val wok: Boolean, val wterm: Long)

        val tests = mutableListOf(
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, 5, true, 1),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, 6, false, 0),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, 4, false, 0),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 5, true, 1),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 6, false, 0),
                // term from snapshot
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 4, true, 1),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 3, false, 0),
                Item(mutableListOf(), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 5, false, 0),
                Item(mutableListOf(), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 4, true, 1),
                Item(mutableListOf(), 0, null, 5, false, 0))

        tests.forEachIndexed { i, tt ->
            val u = Unstable()
            u.entries = tt.entries
            u.offset = tt.offset
            u.snapshot = tt.snap

            val result = u.maybeTerm(tt.index)
            assertEquals(tt.wok, result.isSuccess)
            assertEquals(tt.wterm, result.data, "index:$i is not equal")
        }
    }

    @Test
    fun testUnstableRestore() {
        val unstable = Unstable()
        unstable.entries = mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build())
        unstable.offset = 5
        unstable.snapshot = RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build()
        val s = RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(6).setTerm(2).build()).build()
        unstable.restore(s)

        assertEquals(s.metadata.index + 1, unstable.offset)
        assertEquals(0, CollectionUtils.size(unstable.entries))
        assertEquals(s, unstable.snapshot)
    }

    @Test
    fun testUnstableStableTo() {
        class Item(val entries: List<RaftProtoBuf.Entry>, val offset: Long, val snap: RaftProtoBuf.Snapshot?, val index: Long, val term: Long, val woffset: Long, val wlen: Long)

        val tests = mutableListOf(
                Item(mutableListOf(), 0, null, 5, 1, 0, 0L),
                Item(mutableListOf(buildEntry(5, 1)), 5, null, 5, 1, 6, 0L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build(), RaftProtoBuf.Entry.newBuilder().setIndex(6).setTerm(1).build()), 5, null, 5, 1, 6, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(6).setTerm(2).build()), 6, null, 6, 1, 6, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, 4, 1, 5, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, null, 4, 2, 5, 1L),
                // with snapshot
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 5, 1, 6, 0L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build(), RaftProtoBuf.Entry.newBuilder().setIndex(6).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 5, 1, 6, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(6).setTerm(2).build()), 6, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(5).setTerm(1).build()).build(), 6, 1, 6, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(1).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(), 4, 1, 5, 1L),
                Item(mutableListOf(RaftProtoBuf.Entry.newBuilder().setIndex(5).setTerm(2).build()), 5, RaftProtoBuf.Snapshot.newBuilder().setMetadata(RaftProtoBuf.SnapshotMetadata.newBuilder().setIndex(4).setTerm(2).build()).build(), 4, 1, 5, 1L))

        tests.forEachIndexed { i, tt ->
            val u = Unstable()
            u.entries = tt.entries
            u.offset = tt.offset
            u.snapshot = tt.snap

            u.stableTo(tt.index, tt.term)

            assertEquals(tt.woffset, u.offset)
            assertEquals(tt.wlen, CollectionUtils.size(u.entries).toLong())
        }
    }

    @Test
    fun testUnstableTruncateAndAppend() {
        class Item(val entries: List<RaftProtoBuf.Entry>, val offset: Long, val snap: RaftProtoBuf.Snapshot?, val toappend: List<RaftProtoBuf.Entry>, val woffset: Long, val wentries: List<RaftProtoBuf.Entry>)

        val tests = mutableListOf(
                // append to the end
                Item(mutableListOf(buildEntry(5, 1)), 5, null, mutableListOf<RaftProtoBuf.Entry>(buildEntry(6, 1), buildEntry(7, 1)), 5, mutableListOf(buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1))),
                // replace the unstable entries
                Item(mutableListOf(buildEntry(5, 1)), 5, null, mutableListOf<RaftProtoBuf.Entry>(buildEntry(5, 2), buildEntry(6, 2)), 5, mutableListOf(buildEntry(5, 2), buildEntry(6, 2))),
                Item(mutableListOf(buildEntry(5, 1)), 5, null, mutableListOf<RaftProtoBuf.Entry>(buildEntry(4, 2), buildEntry(5, 2), buildEntry(6, 2)), 4, mutableListOf(buildEntry(4, 2), buildEntry(5, 2), buildEntry(6, 2))),
                // truncate the existing entries and append
                Item(mutableListOf(buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1)), 5, null, mutableListOf<RaftProtoBuf.Entry>(buildEntry(6, 2)), 5, mutableListOf(buildEntry(5, 1), buildEntry(6, 2))),
                Item(mutableListOf(buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1)), 5, null, mutableListOf<RaftProtoBuf.Entry>(buildEntry(7, 2), buildEntry(8, 2)), 5, mutableListOf(buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 2), buildEntry(8, 2))))


        tests.forEachIndexed { i, tt ->
            val u = Unstable()
            u.entries = tt.entries
            u.offset = tt.offset
            u.snapshot = tt.snap

            u.truncateAndAppend(tt.toappend)
            assertEquals(tt.woffset, u.offset)
            assertEquals(tt.wentries, u.entries)
        }
    }

    private fun buildEntry(index: Long, term: Long) = RaftProtoBuf.Entry.newBuilder().setIndex(index).setTerm(term).build()
}