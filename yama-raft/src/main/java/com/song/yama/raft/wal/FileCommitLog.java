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

package com.song.yama.raft.wal;

import com.google.protobuf.ByteString;
import com.song.yama.common.utils.Result;
import com.song.yama.raft.Ready;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.raft.protobuf.WALRecord;
import com.song.yama.raft.protobuf.WALRecord.Record;
import com.song.yama.raft.protobuf.WALRecord.Snapshot;
import com.song.yama.raft.utils.ErrorCode;
import com.song.yama.raft.wal.utils.Constants;
import com.song.yama.raft.wal.utils.Utils;
import com.song.yama.storage.utils.IOUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

@Slf4j
public class FileCommitLog implements CommitLog {

    /**
     * the living directory of the underlay files
     */
    private String dir;

    /**
     * dirFile is a fd for the wal directory for syncing on Rename
     */
    private File dirFile;

    /**
     * metadata recorded at the head of each WAL
     */
    private byte[] metadata;

    /**
     * hardstate recorded at the head of WAL
     */
    private HardState state;

    /**
     * snapshot to start reading
     */
    private WALRecord.Snapshot start;

    private ReentrantLock lock = new ReentrantLock();

    /**
     * index of the last entry saved to the wal
     */
    private long enti;

    private CommitLogSegment commitLogSegment;

    public FileCommitLog() {

    }

    public FileCommitLog(String dir, byte[] metadata) {
        this.dir = dir;
        this.dirFile = new File(this.dir);
        IOUtils.ensureDirOK(this.dir);
        this.metadata = metadata;
    }

    public void init() throws IOException {
        if (Utils.commitLogExists(dir)) {
            List<String> names = IOUtils.listNames(this.dirFile, ".wal");
            if (names.size() != 1) {
                throw new IOException("Found multi wal file!");
            }
            String name = names.get(0);
            commitLogSegment = new CommitLogSegment(dir.concat("/").concat(name), Constants.DEFAULT_WRITE_BUFFER_SIZE);
            commitLogSegment.load();
        } else {
            commitLogSegment = new CommitLogSegment(dir.concat("/").concat(Utils.commitLogName(0, 0)),
                Constants.DEFAULT_WRITE_BUFFER_SIZE);
            commitLogSegment.load();
            commitLogSegment
                .appendRecord(Record.newBuilder().setType(WALRecord.RecordType.Metadata.getNumber())
                    .setData(ByteString.copyFrom(metadata)).build());
            saveSnap(WALRecord.Snapshot.newBuilder().build());
            commitLogSegment.flush();
        }
    }

    @Override
    public Result<Void> save(HardState hardState, List<Entry> ents) {
        this.lock.lock();
        try {
            if (hardState.equals(HardState.getDefaultInstance()) && CollectionUtils.isEmpty(ents)) {
                return Result.success();
            }
            boolean mustSync = Ready.mustSync(hardState, this.state, ents.size());
            for (Entry entry : ents) {
                Result<Void> result = saveEntity(entry);
                if (result.isFailure()) {
                    return result;
                }
            }
            Result<Void> result = saveState(hardState);
            if (result.isFailure()) {
                return result;
            }
            if (mustSync) {
                this.commitLogSegment.flush();
            }
            return Result.success();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return Result.fail(ErrorCode.ErrFlushChannel.getCode(), ErrorCode.ErrFlushChannel.getDesc());
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Result<Void> saveSnap(WALRecord.Snapshot snapshot) {
        this.lock.lock();
        try {
            Record record = Record.newBuilder().setType(WALRecord.RecordType.snapshot.getNumber())
                .setData(snapshot.toByteString()).build();
            Result<Void> result = this.commitLogSegment.appendRecord(record);
            if (result.isFailure()) {
                return result;
            }
            if (this.enti < snapshot.getIndex()) {
                this.enti = snapshot.getIndex();
            }
            this.commitLogSegment.flush();
            return Result.success();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return Result.fail(ErrorCode.ErrFlushChannel.getCode(), ErrorCode.ErrFlushChannel.getDesc());
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Result<RaftStateRecord> readAll(Snapshot snapshot) {
        Result<List<Record>> result = this.commitLogSegment.getRecords();
        if (result.isFailure()) {
            return Result.fail(result.getCode(), result.getMessage());
        }
        HardState hardState = null;
        List<Entry> entries = new ArrayList<>();
        byte[] metadata = null;
        List<Record> records = result.getData();
        records.forEach(record -> {
            long type = record.getType();
            if (type == WALRecord.RecordType.Entry_VALUE) {
                //fuck!
            }
        });
        return null;
    }

    @Override
    public void close() throws IOException {
        this.commitLogSegment.close();
    }

    private Result<Void> saveEntity(Entry entry) {
        Record record = Record.newBuilder().setType(WALRecord.RecordType.Entry.getNumber())
            .setData(entry.toByteString()).build();
        Result<Void> result = this.commitLogSegment.appendRecord(record);
        if (result.isFailure()) {
            return result;
        }
        this.enti = entry.getIndex();
        return result;
    }

    private Result<Void> saveState(HardState hardState) {
        if (com.song.yama.raft.utils.Utils.isEmptyHardState(hardState)) {
            return Result.success();
        }
        this.state = hardState;
        return this.commitLogSegment.appendRecord(
            Record.newBuilder().setType(WALRecord.RecordType.State_VALUE).setData(hardState.toByteString()).build());
    }
}
