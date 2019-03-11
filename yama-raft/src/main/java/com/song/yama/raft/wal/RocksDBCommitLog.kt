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

package com.song.yama.raft.wal

import com.google.common.base.Preconditions.checkArgument
import com.song.yama.common.storage.KeyValueStorage
import com.song.yama.common.storage.impl.RocksDBKeyValueStorage
import com.song.yama.common.utils.Result
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState
import com.song.yama.raft.protobuf.WALRecord
import com.song.yama.raft.protobuf.WALRecord.Snapshot
import com.song.yama.raft.utils.Utils
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.locks.ReentrantLock

class RocksDBCommitLog @Throws(IOException::class)
constructor(path: String) : CommitLog {

    private val lock = ReentrantLock()

    private val keyValueStorage: KeyValueStorage

    /**
     * index of the last entry saved to the wal
     */
    @Volatile
    private var enti: Long = 0

    init {
        checkArgument(StringUtils.isNotBlank(path))
        keyValueStorage = RocksDBKeyValueStorage(path, false)
    }

    override fun save(hardState: HardState, ents: List<Entry>): Result<Void> {
        if (Utils.isEmptyHardState(hardState) && CollectionUtils.isEmpty(ents)) {
            return Result.success()
        }

        this.lock.lock()
        try {
            for (ent in ents) {
                val result = saveEntry(ent)
                if (result.isFailure) {
                    return result
                }
            }

            val result = saveState(hardState)
            if (result.isFailure) {
                return result
            }
        } catch (e: Exception) {
            log.error("Unknown exception when save hard state and entries", e)
            return Result.fail("Unknown error:" + e.message)
        } finally {
            this.lock.unlock()
        }
        return Result.success()
    }

    override fun saveSnap(snapshot: WALRecord.Snapshot): Result<Void> {
        if (snapshot == Snapshot.getDefaultInstance()) {
            log.warn("Ignore empty snapshot:{}.", snapshot)
            return Result.success()
        }
        this.lock.lock()
        try {
            val format = String.format(SNAPSHOT_KEY_PREFIX, snapshot.term, snapshot.index)
            log.info("Write into rocksdb:{}.",format)
            this.keyValueStorage
                    .put(format.toByteArray(),
                            snapshot.toByteArray())
        } catch (e: IOException) {
            log.error("Save snapshot failed:" + snapshot.toString(), e)
            return Result.fail("io error")
        } finally {
            this.lock.unlock()
        }
        return Result.success()
    }

    override fun readAll(snapshot: RaftProtoBuf.Snapshot): Result<RaftStateRecord> {
        val raftStateRecord = RaftStateRecord()
        try {
            val format = String
                    .format(SNAPSHOT_KEY_PREFIX, snapshot.metadata.term, snapshot.metadata.index)
            log.info("Read from rocksdb:{}.", format)
            val data = this.keyValueStorage
                    .get(format
                            .toByteArray())
            if (data == null || data.isEmpty()) {
                return Result.fail("Snapshot not exist")
            }
            val snap = Snapshot.newBuilder().mergeFrom(data).build()
            val term = snap.term
            var index = snap.index
            val stateBytes = this.keyValueStorage.get(STATE_KEY_PREFIX.toByteArray())
            if (stateBytes == null || stateBytes.size == 0) {
                return Result.fail("state not exist")
            }
            raftStateRecord.hardState = HardState.newBuilder().mergeFrom(stateBytes).build()

            val entries = ArrayList<Entry>()
            while (true) {
                val entryBytes = this.keyValueStorage.get(String.format(ENTRY_KEY_PREFIX, term, ++index).toByteArray())
                if (entryBytes == null || entryBytes.isEmpty()) {
                    break
                }
                val e = Entry.newBuilder().mergeFrom(entryBytes).build()
                entries.add(e)
                this.enti = e.index
            }
            raftStateRecord.ents = entries
        } catch (e: Exception) {
            log.error("Read from kv storage failed", e)
            return Result.fail("ReadAll failed :" + e.message)
        }

        return Result.success(raftStateRecord)
    }

    @Throws(IOException::class)
    override fun close() {
        this.keyValueStorage.close()
    }

    private fun saveState(state: HardState): Result<Void> {
        if (Utils.isEmptyHardState(state)) {
            return Result.success()
        }
        try {
            this.keyValueStorage.put(STATE_KEY_PREFIX.toByteArray(), state.toByteArray())
        } catch (e: IOException) {
            log.error("Save state failed:$state", e)
            return Result.fail("io error")
        }

        return Result.success()
    }

    private fun saveEntry(entry: Entry): Result<Void> {
        try {
            this.keyValueStorage.put(String.format(ENTRY_KEY_PREFIX, entry.term, entry.index).toByteArray(),
                    entry.toByteArray())
        } catch (e: IOException) {
            log.error("Save entry failed :$entry", e)
            return Result.fail("io error")
        }

        return Result.success()
    }

    companion object {

        private val log = LoggerFactory.getLogger(RocksDBCommitLog::class.java)

        private const val ENTRY_KEY_PREFIX = "record-entry-%d-%d"

        private const val STATE_KEY_PREFIX = "record-state"

        private const val SNAPSHOT_KEY_PREFIX = "record-snapshot-%d-%d"
    }
}
