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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.song.yama.common.utils.CrcUtils;
import com.song.yama.common.utils.Result;
import com.song.yama.raft.protobuf.WALRecord;
import com.song.yama.raft.protobuf.WALRecord.Record;
import com.song.yama.storage.io.BufferedChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import org.junit.After;
import org.junit.Test;

public class CommitLogSegmentTest {

    @Test
    public void appendRecord() throws IOException {
        File file = File.createTempFile(CommitLogSegmentTest.class.getSimpleName(), ".wal");
        file.deleteOnExit();
        CommitLogSegment commitLogSegment = new CommitLogSegment(file.getAbsolutePath(), 64 * 1024 * 1024);
        commitLogSegment.load();
        byte[] bytes = "Hello World".getBytes();
        Record record = Record.newBuilder().setType(WALRecord.RecordType.Metadata.getNumber()).setData(
            ByteString.copyFrom(bytes)).setCrc(CrcUtils.crc32(bytes)).build();
        Result<Void> result = commitLogSegment
            .appendRecord(record);
        assertThat(result.isSuccess()).isTrue();
        commitLogSegment.flush();
        commitLogSegment.close();

        RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsoluteFile(), "r");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        channel.read(byteBuffer, 0);
        byteBuffer.flip();
        assertThat(byteBuffer.getInt()).isEqualTo(CommitLogSegment.COMMIT_LOG_VERSION);
        byte[] bodyArray = record.toByteArray();
        assertThat(byteBuffer.getInt()).isEqualTo(4 + 4 + 4 + bodyArray.length);
        assertThat(byteBuffer.getInt()).isEqualTo(CrcUtils.crc32(bodyArray));
        int bodySize = byteBuffer.getInt();
        assertThat(bodySize).isEqualTo(bodyArray.length);
        byte[] body = new byte[bodySize];
        byteBuffer.get(body);
        Record r = Record.parseFrom(body);
        assertThat(r).isEqualTo(record);
        channel.close();
        randomAccessFile.close();
    }

    @Test
    public void getRecords() throws IOException {
        File file = File.createTempFile(CommitLogSegmentTest.class.getSimpleName(), ".wal");
        file.deleteOnExit();
        CommitLogSegment commitLogSegment = new CommitLogSegment(file.getAbsolutePath(), 64 * 1024 * 1024);
        commitLogSegment.load();
        for (int i = 0; i < 100; i++) {
            String content = "Hello World" + i;
            byte[] bytes = content.getBytes();
            Record record = Record.newBuilder().setType(WALRecord.RecordType.Metadata.getNumber()).setData(
                ByteString.copyFrom(bytes)).setCrc(CrcUtils.crc32(bytes)).build();
            Result<Void> result = commitLogSegment
                .appendRecord(record);
            assertThat(result.isSuccess()).isTrue();
        }
        commitLogSegment.flush();

        Result<List<Record>> result = commitLogSegment.getRecords();
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getData()).isNotEmpty();
        List<Record> data = result.getData();
        for (int i = 0; i < data.size(); i++) {
            Record record = data.get(i);
            assertThat(record.getData().toStringUtf8()).isEqualTo("Hello World" + i);
        }

        commitLogSegment.close();
    }

    @Test
    public void getRecordsUseBufferedChannel() throws IOException {
        File file = File.createTempFile(CommitLogSegmentTest.class.getSimpleName(), ".wal");
        file.deleteOnExit();
        CommitLogSegment commitLogSegment = new CommitLogSegment(file.getAbsolutePath(), 64 * 1024 * 1024);
        commitLogSegment.load();
        byte[] bytes = "Hello World".getBytes();
        Record record = Record.newBuilder().setType(WALRecord.RecordType.Metadata.getNumber()).setData(
            ByteString.copyFrom(bytes)).setCrc(CrcUtils.crc32(bytes)).build();
        Result<Void> result = commitLogSegment
            .appendRecord(record);
        assertThat(result.isSuccess()).isTrue();
        commitLogSegment.flush();

        BufferedChannel bufferedChannel = new BufferedChannel(commitLogSegment.getFileChannel(), 64 * 1024, 0);
        ByteBuf byteBuf = Unpooled.buffer(1024);
        bufferedChannel.read(byteBuf, 4, 4);
        int size = byteBuf.readInt();
        byteBuf.clear();
        bufferedChannel.read(byteBuf, 8, size - 4);
        byte[] body = record.toByteArray();
        assertThat(byteBuf.readInt()).isEqualTo(CrcUtils.crc32(body));
        assertThat(byteBuf.readInt()).isEqualTo(body.length);
        byte[] b = new byte[body.length];
        byteBuf.readBytes(b, 0, body.length);
        assertThat(Record.parseFrom(b)).isEqualTo(record);
        commitLogSegment.close();
    }

    @After
    public void tearDown() throws Exception {

    }
}