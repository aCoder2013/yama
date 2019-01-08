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

package com.song.yama.raft.wal;

import static com.song.yama.raft.utils.ErrorCode.ErrBrokenFile;
import static com.song.yama.raft.utils.ErrorCode.ErrInvalidCrc;
import static com.song.yama.raft.utils.ErrorCode.ErrInvalidVersion;
import static com.song.yama.raft.utils.ErrorCode.ErrReadFromBufferedChannel;

import com.song.yama.common.utils.CrcUtils;
import com.song.yama.common.utils.Result;
import com.song.yama.raft.protobuf.WALRecord.Record;
import com.song.yama.raft.wal.utils.Constants;
import com.song.yama.storage.io.BufferedChannel;
import com.song.yama.storage.utils.IOUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitLogSegment implements Closeable {

    public static final int COMMIT_LOG_VERSION = 1;

    private String fileName;

    private FileChannel fileChannel;

    private int writeBufferSize;

    private BufferedChannel bufferedChannel;

    public CommitLogSegment(String fileName, int writeBufferSize) {
        this.fileName = fileName;
        this.writeBufferSize = writeBufferSize;
    }

    public void load() throws IOException {
        RandomAccessFile randomAccessFile = null;
        boolean success = false;
        try {
            File file = new File(this.fileName);
            IOUtils.ensureDirOK(file.getParent());
            randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            this.bufferedChannel = new BufferedChannel(this.fileChannel, this.writeBufferSize, 0L);
            ByteBuf headBuf = Unpooled.buffer(4);
            headBuf.writeInt(COMMIT_LOG_VERSION);
            this.bufferedChannel.write(headBuf);
            this.bufferedChannel.flush();
            success = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to open channel :" + fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to create BufferedChannel :" + fileName, e);
            throw e;
        } finally {
            if (!success) {
                if (this.fileChannel != null) {
                    this.fileChannel.close();
                }
                if (this.bufferedChannel != null) {
                    this.bufferedChannel.close();
                }
            }
        }
    }

    public Result<Void> appendRecord(Record record) {
        byte[] body = record.toByteArray();
        final int totalLength =
            4 //total size
                + 4 // crc
                + 4 + body.length; //body
        ByteBuf byteBuf = Unpooled.buffer(totalLength + Constants.END_FILE_MIN_BLANK_LENGTH);
        byteBuf.retain();
        byteBuf.writeInt(totalLength);
        byteBuf.writeInt(CrcUtils.crc32(body));
        byteBuf.writeInt(body.length);
        byteBuf.writeBytes(body);
        try {
            this.bufferedChannel.write(byteBuf);
            return Result.success();
        } catch (IOException e) {
            log.error("Write to buffered channel failed_" + e.getMessage(), e);
            return Result.fail(e);
        } finally {
            byteBuf.release();
        }
    }

    public Result<List<Record>> getRecords() {
        ByteBuf headerBuf = Unpooled.buffer(4);
        try {
            int currentReadPostion = 0;
            long size = this.bufferedChannel.size();
            if (size < 4) {
                log.warn("Commit log is broken,size:{}.", size);
                return Result.fail(ErrBrokenFile.getCode(), ErrBrokenFile.getDesc());
            }
            this.bufferedChannel.read(headerBuf, 0, 4);
            currentReadPostion += 4;
            int version = headerBuf.readInt();
            if (COMMIT_LOG_VERSION != version) {
                log.warn("Invalid commit log,wrong version ,expected:{},actual:{}.", COMMIT_LOG_VERSION, version);
                return Result.fail(ErrInvalidVersion.getCode(), ErrInvalidVersion.getDesc());
            }
            List<Record> records = new ArrayList<>();
            ByteBuf lengthBuf = Unpooled.buffer(4);
            while (currentReadPostion < size) {
                this.bufferedChannel.read(lengthBuf, currentReadPostion, 4);
                currentReadPostion += 4;
                //TODO:optimize usage of ByteBuf,try to reuse is
                //FIXME:limit max message size
                int totalLength = lengthBuf.readInt();
                ByteBuf recordBuf = Unpooled.buffer(totalLength);
                this.bufferedChannel.read(recordBuf, currentReadPostion, totalLength - 4);//minus length's size
                int crc = recordBuf.readInt();
                int bodyLength = recordBuf.readInt();
                byte[] bodyBytes = new byte[bodyLength];
                recordBuf.readBytes(bodyBytes, 0, bodyBytes.length);
                int actualCrc = CrcUtils.crc32(bodyBytes);
                if (crc != actualCrc) {
                    log.error("Record crc check failed,expected:{},actual:{}.", crc, actualCrc);
                    return Result.fail(ErrInvalidCrc.getCode(), ErrInvalidCrc.getDesc());
                }
                Record record = Record.parseFrom(bodyBytes);
                //TODO:check record#data crc
                records.add(record);
                currentReadPostion -= 4;
                currentReadPostion += totalLength;
                lengthBuf.clear();
            }
            return Result.success(records);
        } catch (IOException e) {
            log.error("Read from buffered channel failed", e);
            return Result.fail(ErrReadFromBufferedChannel.getCode(), ErrReadFromBufferedChannel.getDesc());
        }
    }

    public void flush() throws IOException {
        this.bufferedChannel.flush();
    }

    @Override
    public void close() throws IOException {
        this.bufferedChannel.close();
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }
}
