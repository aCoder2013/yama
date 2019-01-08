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

import static com.song.yama.raft.wal.utils.Utils.commitLogName;

import com.song.yama.common.utils.Result;
import com.song.yama.raft.protobuf.WALRecord.Snapshot;
import com.song.yama.raft.utils.ErrorCode;
import com.song.yama.raft.wal.utils.Constants;
import com.song.yama.raft.wal.utils.Utils;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitLogFactory {

    /**
     * create a file commit log ready for appending records. The given metadata is recorded at the head of each commit
     * log, and can be retrieved with {@link CommitLog#readAll(Snapshot)}}
     */
    public Result<CommitLog> newFileCommitLog(String dir, byte[] metadata) {
        if (Utils.commitLogExists(dir)) {
            return Result.fail(ErrorCode.ErrExist.getCode(), ErrorCode.ErrExist.getDesc());
        }

        RandomAccessFile file = null;
        FileChannel fileChannel = null;
        try {
            file = new RandomAccessFile(dir.concat(commitLogName(0, 0)), "rw");
            fileChannel = file.getChannel();
            FileLock fileLock = fileChannel.tryLock();
            if (fileLock == null) {
                return Result.fail("Lock could not be acquired");
            } else {
                MappedByteBuffer mappedByteBuffer = fileChannel
                    .map(MapMode.READ_WRITE, 0, Constants.COMMIT_LOG_FILE_SIZE);
                CommitLog commitLog = new FileCommitLog(dir, metadata);

            }
        } catch (Exception e) {
            log.error("Failed to create commit log_" + e.getMessage(), e);
            return Result.fail("Failed to create commit log:" + e.getMessage());
        }
        return null;
    }
}
