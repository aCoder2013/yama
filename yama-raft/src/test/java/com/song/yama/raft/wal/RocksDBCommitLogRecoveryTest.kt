package com.song.yama.raft.wal

import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
import com.song.yama.raft.protobuf.RaftProtoBuf.EntryType
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot
import com.song.yama.raft.protobuf.RaftProtoBuf.SnapshotMetadata
import com.song.yama.raft.protobuf.WALRecord
import com.song.yama.raft.utils.ProtoBufUtils.buildEntry
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class RocksDBCommitLogRecoveryTest {

    private lateinit var walDir: File
    private lateinit var commitLog: RocksDBCommitLog

    @Before
    fun setUp() {
        walDir = Files.createTempDirectory("yama-wal-test").toFile()
        commitLog = RocksDBCommitLog(walDir.absolutePath)
    }

    @After
    fun tearDown() {
        try {
            commitLog.close()
        } catch (ignored: Exception) {
        }
        walDir.deleteRecursively()
    }

    @Test
    fun readAllOnEmptyWal() {
        val result = commitLog.readAll()
        assertTrue(result.isSuccess)
        val record = result.data!!
        val ents = record.ents!!
        assertTrue(ents.isEmpty())
    }

    @Test
    fun walOnlyRecoveryPreservesEntriesAcrossTerms() {
        val hardState = HardState.newBuilder().setTerm(2).setVote(1).setCommit(3).build()
        val entries = listOf(
            buildEntry("{\"key\":\"a\",\"value\":\"1\"}".toByteArray()).toBuilder().setIndex(1).setTerm(1).build(),
            buildEntry("{\"key\":\"b\",\"value\":\"2\"}".toByteArray()).toBuilder().setIndex(2).setTerm(1).build(),
            buildEntry("{\"key\":\"c\",\"value\":\"3\"}".toByteArray()).toBuilder().setIndex(3).setTerm(2).build()
        )
        assertTrue(commitLog.save(hardState, entries).isSuccess)

        commitLog.close()
        val reopened = RocksDBCommitLog(walDir.absolutePath)
        val result = reopened.readAll()
        reopened.close()

        assertTrue(result.isSuccess)
        val record = result.data!!
        val ents = record.ents!!
        assertEquals(3, ents.size)
        assertEquals(3, record.hardState!!.commit)
        assertEquals(1, ents[0].index)
        assertEquals(1, ents[1].term)
        assertEquals(3, ents[2].index)
        assertEquals(2, ents[2].term)
        assertEquals("{\"key\":\"c\",\"value\":\"3\"}", ents[2].data.toStringUtf8())
    }

    @Test
    fun snapshotPlusWalRecoveryReadsPostSnapshotEntries() {
        val snapshot = Snapshot.newBuilder()
            .setMetadata(SnapshotMetadata.newBuilder().setIndex(5).setTerm(2).build())
            .setData(com.google.protobuf.ByteString.copyFrom("{\"k\":\"v\"}".toByteArray()))
            .build()
        val snapRecord = WALRecord.Snapshot.newBuilder().setIndex(5).setTerm(2).build()
        assertTrue(commitLog.saveSnap(snapRecord).isSuccess)

        val postSnapshot = listOf(
            Entry.newBuilder()
                .setType(EntryType.EntryNormal)
                .setIndex(6)
                .setTerm(3)
                .setData(com.google.protobuf.ByteString.copyFrom("after-snap".toByteArray()))
                .build()
        )
        val hardState = HardState.newBuilder().setTerm(3).setVote(1).setCommit(6).build()
        assertTrue(commitLog.save(hardState, postSnapshot).isSuccess)

        commitLog.close()
        val reopened = RocksDBCommitLog(walDir.absolutePath)
        val result = reopened.readAll(snapshot)
        reopened.close()

        assertTrue(result.isSuccess)
        val record = result.data!!
        val ents = record.ents!!
        assertEquals(1, ents.size)
        assertEquals(6, ents[0].index)
        assertEquals(3, ents[0].term)
        assertEquals("after-snap", ents[0].data.toStringUtf8())
    }

    @Test
    fun entryKeyIsIndexOnlyNotTermBound() {
        val e1 = buildEntry("x".toByteArray()).toBuilder().setIndex(1).setTerm(1).build()
        val e2 = buildEntry("y".toByteArray()).toBuilder().setIndex(2).setTerm(5).build()
        assertTrue(commitLog.save(HardState.newBuilder().setTerm(5).setCommit(2).build(), listOf(e1, e2)).isSuccess)

        commitLog.close()
        val reopened = RocksDBCommitLog(walDir.absolutePath)
        val result = reopened.readAll()
        reopened.close()

        val record = result.data!!
        val ents = record.ents!!
        assertEquals(2, ents.size)
        assertEquals(5, ents[1].term)
    }
}
