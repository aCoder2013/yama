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

import static com.google.common.base.Preconditions.checkArgument;

import com.song.yama.common.storage.KeyValueStorage;
import com.song.yama.common.storage.impl.RocksDBKeyValueStorage;
import com.song.yama.common.utils.Result;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.raft.protobuf.WALRecord;
import com.song.yama.raft.protobuf.WALRecord.Snapshot;
import com.song.yama.raft.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class RocksDBCommitLog implements CommitLog {

    private static final String ENTRY_KEY_PREFIX = "record-entry-%d-%d";

    private static final String STATE_KEY_PREFIX = "record-state";

    private static final String SNAPSHOT_KEY_PREFIX = "record-snapshot-%d-%d";

    private final Lock lock = new ReentrantLock();

    private KeyValueStorage keyValueStorage;

    /**
     * index of the last entry saved to the wal
     */
    private volatile long enti;

    public RocksDBCommitLog(String path) throws IOException {
        checkArgument(StringUtils.isNotBlank(path));
        keyValueStorage = new RocksDBKeyValueStorage(path, false);
    }

    @Override
    public Result<Void> save(HardState hardState, List<Entry> ents) {
        if (Utils.isEmptyHardState(hardState) && CollectionUtils.isEmpty(ents)) {
            return Result.success();
        }

        this.lock.lock();
        try {
            for (Entry ent : ents) {
                Result<Void> result = saveEntry(ent);
                if (result.isFailure()) {
                    return result;
                }
            }

            Result<Void> result = saveState(hardState);
            if (result.isFailure()) {
                return result;
            }
        } catch (Exception e) {
            log.error("Unknown exception when save hard state and entries", e);
            return Result.fail("Unknown error:" + e.getMessage());
        } finally {
            this.lock.unlock();
        }
        return Result.success();
    }

    @Override
    public Result<Void> saveSnap(WALRecord.Snapshot snapshot) {
        if (snapshot == null || snapshot.equals(Snapshot.getDefaultInstance())) {
            log.warn("Ignore empty snapshot:{}.", snapshot);
            return Result.success();
        }
        this.lock.lock();
        try {
            this.keyValueStorage
                .put(String.format(SNAPSHOT_KEY_PREFIX, snapshot.getTerm(), snapshot.getTerm()).getBytes(),
                    snapshot.toByteArray());
        } catch (IOException e) {
            log.error("Save snapshot failed:" + snapshot.toString(), e);
            return Result.fail("io error");
        } finally {
            this.lock.unlock();
        }
        return Result.success();
    }

    @Override
    public Result<RaftStateRecord> readAll(Snapshot snapshot) {
        RaftStateRecord raftStateRecord = new RaftStateRecord();
        try {
            byte[] data = this.keyValueStorage
                .get(String.format(SNAPSHOT_KEY_PREFIX, snapshot.getTerm(), snapshot.getIndex()).getBytes());
            if (data == null || data.length == 0) {
                return Result.fail("Snapshot not exist");
            }
            Snapshot snap = Snapshot.newBuilder().mergeFrom(data).build();
            long term = snap.getTerm();
            long index = snap.getIndex();
            byte[] stateBytes = this.keyValueStorage.get(STATE_KEY_PREFIX.getBytes());
            if (stateBytes == null || stateBytes.length == 0) {
                return Result.fail("state not exist");
            }
            raftStateRecord.setHardState(HardState.newBuilder().mergeFrom(stateBytes).build());

            List<Entry> entries = new ArrayList<>();
            while (true) {
                byte[] entryBytes = this.keyValueStorage.get(String.format(ENTRY_KEY_PREFIX, term, ++index).getBytes());
                if (entryBytes == null || entryBytes.length == 0) {
                    break;
                }
                Entry e = Entry.newBuilder().mergeFrom(entryBytes).build();
                entries.add(e);
                this.enti = e.getIndex();
            }
            raftStateRecord.setEnts(entries);
        } catch (Exception e) {
            log.error("Read from kv storage failed", e);
            return Result.fail("ReadAll failed :" + e.getMessage());
        }
        return Result.success(raftStateRecord);
    }

    @Override
    public void close() throws IOException {
        this.keyValueStorage.close();
    }

    private Result<Void> saveState(HardState state) {
        if (Utils.isEmptyHardState(state)) {
            return Result.success();
        }
        try {
            this.keyValueStorage.put(STATE_KEY_PREFIX.getBytes(), state.toByteArray());
        } catch (IOException e) {
            log.error("Save state failed:" + state, e);
            return Result.fail("io error");
        }
        return Result.success();
    }


    private Result<Void> saveEntry(Entry entry) {
        try {
            this.keyValueStorage.put(String.format(ENTRY_KEY_PREFIX, entry.getTerm(), entry.getIndex()).getBytes(),
                entry.toByteArray());
        } catch (IOException e) {
            log.error("Save entry failed :" + entry, e);
            return Result.fail("io error");
        }
        return Result.success();
    }
}
