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
//package com.song.yama.raft.wal
//
//import com.song.yama.common.utils.Result
//import com.song.yama.raft.utils.ErrorCode
//import com.song.yama.raft.wal.utils.Constants
//import com.song.yama.raft.wal.utils.Utils
//import com.song.yama.raft.wal.utils.Utils.commitLogName
//import lombok.extern.slf4j.Slf4j
//import org.slf4j.LoggerFactory
//import java.io.RandomAccessFile
//import java.nio.channels.FileChannel
//import java.nio.channels.FileChannel.MapMode
//
//@Slf4j
//class CommitLogFactory {
//
//    /**
//     * create a file commit log ready for appending records. The given metadata is recorded at the head of each commit
//     * log, and can be retrieved with [CommitLog.readAll]}
//     */
//    fun newFileCommitLog(dir: String, metadata: ByteArray): Result<CommitLog>? {
//        if (Utils.commitLogExists(dir)) {
//            return Result.fail(ErrorCode.ErrExist.code, ErrorCode.ErrExist.desc)
//        }
//
//        var file: RandomAccessFile? = null
//        var fileChannel: FileChannel? = null
//        try {
//            file = RandomAccessFile(dir + commitLogName(0, 0), "rw")
//            fileChannel = file.channel
//            val fileLock = fileChannel!!.tryLock()
//            if (fileLock == null) {
//                return Result.fail("Lock could not be acquired")
//            } else {
//                val mappedByteBuffer = fileChannel
//                        .map(MapMode.READ_WRITE, 0, Constants.COMMIT_LOG_FILE_SIZE.toLong())
//                val commitLog = FileCommitLog(dir, metadata)
//
//            }
//        } catch (e: Exception) {
//            log.error("Failed to create commit log_" + e.message, e)
//            return Result.fail("Failed to create commit log:" + e.message)
//        }
//
//        return null
//    }
//
//    companion object {
//        private val log = LoggerFactory.getLogger(CommitLogFactory::class.java)
//    }
//}
