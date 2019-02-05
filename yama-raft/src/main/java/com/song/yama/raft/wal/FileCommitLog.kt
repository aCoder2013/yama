///*
// *  Copyright 2018 acoder2013
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *      http:www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// */
//
//// Copyright 2015 The etcd Authors
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
//package com.song.yama.raft.wal
//
//import com.google.protobuf.ByteString
//import com.song.yama.common.utils.Result
//import com.song.yama.raft.Ready
//import com.song.yama.raft.protobuf.RaftProtoBuf
//import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
//import com.song.yama.raft.protobuf.RaftProtoBuf.HardState
//import com.song.yama.raft.protobuf.WALRecord
//import com.song.yama.raft.protobuf.WALRecord.Record
//import com.song.yama.raft.utils.ErrorCode
//import com.song.yama.raft.wal.utils.Constants
//import com.song.yama.raft.wal.utils.Utils
//import com.song.yama.storage.utils.IOUtils
//import org.apache.commons.collections4.CollectionUtils
//import org.slf4j.LoggerFactory
//import java.io.File
//import java.io.IOException
//import java.util.*
//import java.util.concurrent.locks.ReentrantLock
//
//class FileCommitLog : CommitLog {
//
//    /**
//     * the living directory of the underlay files
//     */
//    private var dir: String? = null
//
//    /**
//     * dirFile is a fd for the wal directory for syncing on Rename
//     */
//    private var dirFile: File? = null
//
//    /**
//     * metadata recorded at the head of each WAL
//     */
//    private var metadata: ByteArray? = null
//
//    /**
//     * hardstate recorded at the head of WAL
//     */
//    private var state: HardState? = null
//
//    /**
//     * snapshot to start reading
//     */
//    private val start: WALRecord.Snapshot? = null
//
//    private val lock = ReentrantLock()
//
//    /**
//     * index of the last entry saved to the wal
//     */
//    private var enti: Long = 0
//
//    private var commitLogSegment: CommitLogSegment? = null
//
//    constructor() {
//
//    }
//
//    constructor(dir: String, metadata: ByteArray) {
//        this.dir = dir
//        this.dirFile = File(this.dir)
//        IOUtils.ensureDirOK(this.dir)
//        this.metadata = metadata
//    }
//
//    @Throws(IOException::class)
//    fun init() {
//        if (Utils.commitLogExists(dir)) {
//            val names = IOUtils.listNames(this.dirFile, ".wal")
//            if (names.size != 1) {
//                throw IOException("Found multi wal file!")
//            }
//            val name = names[0]
//            commitLogSegment = CommitLogSegment("$dir/$name", Constants.DEFAULT_WRITE_BUFFER_SIZE)
//            commitLogSegment!!.load()
//        } else {
//            commitLogSegment = CommitLogSegment(dir + "/" + Utils.commitLogName(0, 0),
//                    Constants.DEFAULT_WRITE_BUFFER_SIZE)
//            commitLogSegment!!.load()
//            commitLogSegment!!
//                    .appendRecord(Record.newBuilder().setType(WALRecord.RecordType.Metadata.number.toLong())
//                            .setData(ByteString.copyFrom(metadata)).build())
//            saveSnap(WALRecord.Snapshot.newBuilder().build())
//            commitLogSegment!!.flush()
//        }
//    }
//
//    override fun save(hardState: HardState, ents: List<Entry>): Result<Void> {
//        this.lock.lock()
//        try {
//            if (hardState == HardState.getDefaultInstance() && CollectionUtils.isEmpty(ents)) {
//                return Result.success()
//            }
//            val mustSync = Ready.mustSync(hardState, this.state!!, ents.size)
//            for (entry in ents) {
//                val result = saveEntity(entry)
//                if (result.isFailure) {
//                    return result
//                }
//            }
//            val result = saveState(hardState)
//            if (result.isFailure) {
//                return result
//            }
//            if (mustSync) {
//                this.commitLogSegment!!.flush()
//            }
//            return Result.success()
//        } catch (e: IOException) {
//            log.error(e.message, e)
//            return Result.fail(ErrorCode.ErrFlushChannel.code, ErrorCode.ErrFlushChannel.desc)
//        } finally {
//            this.lock.unlock()
//        }
//    }
//
//    override fun saveSnap(snapshot: WALRecord.Snapshot): Result<Void> {
//        this.lock.lock()
//        try {
//            val record = Record.newBuilder().setType(WALRecord.RecordType.snapshot.number.toLong())
//                    .setData(snapshot.toByteString()).build()
//            val result = this.commitLogSegment!!.appendRecord(record)
//            if (result.isFailure) {
//                return result
//            }
//            if (this.enti < snapshot.index) {
//                this.enti = snapshot.index
//            }
//            this.commitLogSegment!!.flush()
//            return Result.success()
//        } catch (e: IOException) {
//            log.error(e.message, e)
//            return Result.fail(ErrorCode.ErrFlushChannel.code, ErrorCode.ErrFlushChannel.desc)
//        } finally {
//            this.lock.unlock()
//        }
//    }
//
//    override fun readAll(snapshot: RaftProtoBuf.Snapshot): Result<RaftStateRecord> {
//        val result = this.commitLogSegment!!.records
//        if (result.isFailure) {
//            return Result.fail(result.code, result.message)
//        }
//        val hardState: HardState? = null
//        val entries = ArrayList<Entry>()
//        val metadata: ByteArray? = null
//        val records = result.data
//        records.forEach { record ->
//            val type = record.type
//            if (type == WALRecord.RecordType.Entry_VALUE.toLong()) {
//                //fuck!
//            }
//        }
//        return Result.fail()
//    }
//
//    @Throws(IOException::class)
//    override fun close() {
//        this.commitLogSegment!!.close()
//    }
//
//    private fun saveEntity(entry: Entry): Result<Void> {
//        val record = Record.newBuilder().setType(WALRecord.RecordType.Entry.number.toLong())
//                .setData(entry.toByteString()).build()
//        val result = this.commitLogSegment!!.appendRecord(record)
//        if (result.isFailure) {
//            return result
//        }
//        this.enti = entry.index
//        return result
//    }
//
//    private fun saveState(hardState: HardState): Result<Void> {
//        if (com.song.yama.raft.utils.Utils.isEmptyHardState(hardState)) {
//            return Result.success()
//        }
//        this.state = hardState
//        return this.commitLogSegment!!.appendRecord(
//                Record.newBuilder().setType(WALRecord.RecordType.State_VALUE.toLong()).setData(hardState.toByteString()).build())
//    }
//
//    companion object {
//
//        private val log = LoggerFactory.getLogger(FileCommitLog::class.java)
//
//    }
//}
