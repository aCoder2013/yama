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

import com.song.yama.common.utils.CrcUtils
import com.song.yama.common.utils.Result
import com.song.yama.raft.protobuf.WALRecord.Record
import com.song.yama.raft.utils.ErrorCode.*
import com.song.yama.raft.wal.utils.Constants
import com.song.yama.storage.io.BufferedChannel
import com.song.yama.storage.utils.IOUtils
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.io.*
import java.nio.channels.FileChannel
import java.util.*

class CommitLogSegment(private val fileName: String, private val writeBufferSize: Int) : Closeable {

    var fileChannel: FileChannel? = null
        private set

    private var bufferedChannel: BufferedChannel? = null

    //TODO:optimize usage of ByteBuf,try to reuse is
    //FIXME:limit max message size
    //minus length's size
    //TODO:check record#data crc
    val records: Result<List<Record>>
        get() {
            val headerBuf = Unpooled.buffer(4)
            try {
                var currentReadPostion = 0
                val size = this.bufferedChannel!!.size()
                if (size < 4) {
                    log.warn("Commit log is broken,size:{}.", size)
                    return Result.fail(ErrBrokenFile.code, ErrBrokenFile.desc)
                }
                this.bufferedChannel!!.read(headerBuf, 0, 4)
                currentReadPostion += 4
                val version = headerBuf.readInt()
                if (COMMIT_LOG_VERSION != version) {
                    log.warn("Invalid commit log,wrong version ,expected:{},actual:{}.", COMMIT_LOG_VERSION, version)
                    return Result.fail(ErrInvalidVersion.code, ErrInvalidVersion.desc)
                }
                val records = ArrayList<Record>()
                val lengthBuf = Unpooled.buffer(4)
                while (currentReadPostion < size) {
                    this.bufferedChannel!!.read(lengthBuf, currentReadPostion.toLong(), 4)
                    currentReadPostion += 4
                    val totalLength = lengthBuf.readInt()
                    val recordBuf = Unpooled.buffer(totalLength)
                    this.bufferedChannel!!.read(recordBuf, currentReadPostion.toLong(), totalLength - 4)
                    val crc = recordBuf.readInt()
                    val bodyLength = recordBuf.readInt()
                    val bodyBytes = ByteArray(bodyLength)
                    recordBuf.readBytes(bodyBytes, 0, bodyBytes.size)
                    val actualCrc = CrcUtils.crc32(bodyBytes)
                    if (crc != actualCrc) {
                        log.error("Record crc check failed,expected:{},actual:{}.", crc, actualCrc)
                        return Result.fail(ErrInvalidCrc.code, ErrInvalidCrc.desc)
                    }
                    val record = Record.parseFrom(bodyBytes)
                    records.add(record)
                    currentReadPostion -= 4
                    currentReadPostion += totalLength
                    lengthBuf.clear()
                }
                return Result.success(records)
            } catch (e: IOException) {
                log.error("Read from buffered channel failed", e)
                return Result.fail(ErrReadFromBufferedChannel.code, ErrReadFromBufferedChannel.desc)
            }

        }

    @Throws(IOException::class)
    fun load() {
        var randomAccessFile: RandomAccessFile? = null
        var success = false
        try {
            val file = File(this.fileName)
            IOUtils.ensureDirOK(file.parent)
            randomAccessFile = RandomAccessFile(file, "rw")
            this.fileChannel = randomAccessFile.channel
            this.bufferedChannel = BufferedChannel(this.fileChannel, this.writeBufferSize, 0L)
            val headBuf = Unpooled.buffer(4)
            headBuf.writeInt(COMMIT_LOG_VERSION)
            this.bufferedChannel!!.write(headBuf)
            this.bufferedChannel!!.flush()
            success = true
        } catch (e: FileNotFoundException) {
            log.error("Failed to open channel :$fileName", e)
            throw e
        } catch (e: IOException) {
            log.error("Failed to create BufferedChannel :$fileName", e)
            throw e
        } finally {
            if (!success) {
                if (this.fileChannel != null) {
                    this.fileChannel!!.close()
                }
                if (this.bufferedChannel != null) {
                    this.bufferedChannel!!.close()
                }
            }
        }
    }

    fun appendRecord(record: Record): Result<Void> {
        val body = record.toByteArray()
        val totalLength = (4 //total size

                + 4 // crc

                + 4 + body.size) //body
        val byteBuf = Unpooled.buffer(totalLength + Constants.END_FILE_MIN_BLANK_LENGTH)
        byteBuf.retain()
        byteBuf.writeInt(totalLength)
        byteBuf.writeInt(CrcUtils.crc32(body))
        byteBuf.writeInt(body.size)
        byteBuf.writeBytes(body)
        try {
            this.bufferedChannel!!.write(byteBuf)
            return Result.success()
        } catch (e: IOException) {
            log.error("Write to buffered channel failed_" + e.message, e)
            return Result.fail(e)
        } finally {
            byteBuf.release()
        }
    }

    @Throws(IOException::class)
    fun flush() {
        this.bufferedChannel!!.flush()
    }

    @Throws(IOException::class)
    override fun close() {
        this.bufferedChannel!!.close()
    }

    companion object {

        private val log = LoggerFactory.getLogger(CommitLogSegment::class.java)

        val COMMIT_LOG_VERSION = 1
    }

}
