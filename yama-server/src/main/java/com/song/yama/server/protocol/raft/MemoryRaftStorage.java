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

package com.song.yama.server.protocol.raft;

import com.google.protobuf.ByteString;
import com.song.yama.common.utils.Result;
import com.song.yama.server.protocol.raft.exception.RaftException;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.ConfState;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.Snapshot.Builder;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.SnapshotMetadata;
import com.song.yama.server.protocol.raft.utils.ErrorCode;
import com.song.yama.server.protocol.raft.utils.ProtoBufUtils;
import com.song.yama.server.protocol.raft.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

/**
 * a simple Raft Storage based memory,just for test.
 */
@Slf4j
public class MemoryRaftStorage implements RaftStorage {

    private final Lock lock = new ReentrantLock();

    private HardState hardState = HardState.newBuilder().build();

    private Snapshot snapshot = Snapshot.newBuilder().build();

    // ents[i] has raft log position i+snapshot.Metadata.Index
    private List<Entry> ents = new ArrayList<>(Collections.singletonList(Entry.newBuilder().build()));

    public MemoryRaftStorage() {
    }

    public MemoryRaftStorage(List<Entry> ents) {
        this.ents = ents;
    }

    @Override
    public RaftState initialState() {
        return new RaftState(this.hardState, this.snapshot.getMetadata().getConfState());
    }

    @Override
    public Result<List<Entry>> entries(long low, long high, long maxSize) {
        this.lock.lock();
        try {
            long offset = this.ents.get(0).getIndex();
            if (low <= offset) {
                return Result.fail(ErrorCode.ErrCompacted.getCode(), ErrorCode.ErrCompacted.getDesc());
            }

            if (high > lastIndex() + 1) {
                throw new RaftException(
                    String.format("entries' hi(%d) is out of bound lastindex(%d)", high, lastIndex()));
            }

            //on contains dummy entries
            if (ents.size() == 1) {
                return Result.fail(ErrorCode.ErrUnavailable.getCode(), ErrorCode.ErrUnavailable.getDesc());
            }
            List<Entry> entries = new ArrayList<>(this.ents.subList((int) (low - offset), (int) (high - offset)));
            return Result.success(Utils.limitSize(entries, maxSize));
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Result<Long> term(long i) {
        this.lock.lock();
        try {
            long offset = this.ents.get(0).getIndex();
            if (i < offset) {
                return Result.fail(ErrorCode.ErrCompacted.getCode(), ErrorCode.ErrCompacted.getDesc(), 0L);
            }

            if ((int) (i - offset) >= this.ents.size()) {
                return Result.fail(ErrorCode.ErrUnavailable.getCode(), ErrorCode.ErrUnavailable.getDesc(), 0L);
            }
            return Result.success(this.ents.get((int) (i - offset)).getTerm());
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long lastIndex() {
        this.lock.lock();
        try {
            return this.ents.get(0).getIndex() + this.ents.size() - 1;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long firstIndex() {
        this.lock.lock();
        try {
            return this.ents.get(0).getIndex() + 1;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Result<Snapshot> snapshot() {
        this.lock.lock();
        try {
            return Result.success(this.snapshot);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Result<Void> applySnapshot(Snapshot snapshot) {
        this.lock.lock();
        try {
            //handle check for old snapshot being applied
            long msIndex = this.snapshot.getMetadata().getIndex();
            long snapIndex = snapshot.getMetadata().getIndex();
            if (msIndex >= snapIndex) {
                return Result.fail(ErrorCode.ErrSnapOutOfDate.getCode(), ErrorCode.ErrSnapOutOfDate.getDesc());
            }

            this.snapshot = snapshot;
            this.ents = new ArrayList<>(Collections.singletonList(
                ProtoBufUtils.buildEntry(snapshot.getMetadata().getIndex(), snapshot.getMetadata().getTerm())));
        } finally {
            this.lock.unlock();
        }
        return Result.success();
    }

    @Override
    public Result<Snapshot> createSnapshot(long i, ConfState cs, byte[] data) {
        this.lock.lock();
        try {
            if (i <= this.snapshot.getMetadata().getIndex()) {
                return Result.fail(ErrorCode.ErrSnapOutOfDate.getCode(), ErrorCode.ErrSnapOutOfDate.getDesc(),
                    Snapshot.getDefaultInstance());
            }

            long offset = this.ents.get(0).getIndex();
            if (i > lastIndex()) {
                throw new RaftException(String.format("snapshot %d is out of bound lastindex(%d)", i, lastIndex()));
            }

            Builder snapBuilder = this.snapshot.toBuilder();
            SnapshotMetadata.Builder metadataBuilder = this.snapshot.getMetadata().toBuilder()
                .setIndex(i)
                .setTerm(this.ents.get((int) (i - offset)).getTerm());
            if (cs != null) {
                metadataBuilder.setConfState(cs);
            }
            snapBuilder.setMetadata(metadataBuilder.build());
            snapBuilder.setData(ByteString.copyFrom(data));
            this.snapshot = snapBuilder.build();
        } finally {
            this.lock.unlock();
        }
        return Result.success(this.snapshot);
    }

    @Override
    public Result<Void> compact(long compactIndex) {
        this.lock.lock();
        try {
            long offset = this.ents.get(0).getIndex();
            if (compactIndex <= offset) {
                return Result.fail(ErrorCode.ErrCompacted.getCode(), ErrorCode.ErrCompacted.getDesc());
            }

            if (compactIndex > lastIndex()) {
                String message = String.format("compact %d is out of bound lastindex(%d)", compactIndex, lastIndex());
                log.warn(message);
                throw new RaftException(message);
            }

            int i = (int) (compactIndex - offset);

            List<Entry> ents = new ArrayList<>(1 + this.ents.size() - i);
            ents.add(ProtoBufUtils.buildEntry(this.ents.get(i).getIndex(), this.ents.get(i).getTerm()));
            ents.addAll(this.ents.subList(i + 1, this.ents.size()));
            this.ents = ents;
        } finally {
            this.lock.unlock();
        }
        return Result.success();
    }

    @Override
    public Result<Void> append(List<Entry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return Result.success();
        }
        this.lock.lock();
        try {
            long first = firstIndex();
            long last = entries.get(0).getIndex() + entries.size() - 1;

            // shortcut if there is no new entry.
            if (last < first) {
                return Result.success();
            }

            // truncate compacted entries
            if (first > entries.get(0).getIndex()) {
                entries = new ArrayList<>(entries.subList((int) (first - entries.get(0).getIndex()), entries.size()));
            }

            long offset = entries.get(0).getIndex() - this.ents.get(0).getIndex();
            if (this.ents.size() > offset) {
                this.ents = new ArrayList<>(this.ents.subList(0, (int) offset));
                this.ents.addAll(entries);
            } else if (this.ents.size() == offset) {
                this.ents.addAll(entries);
            } else {
                throw new RaftException(String
                    .format("missing log entry [last: %d, append at: %d]", lastIndex(), entries.get(0).getIndex()));
            }
        } finally {
            this.lock.unlock();
        }
        return Result.success();
    }

    @Override
    public void setHardState(HardState hardState) {
        this.lock.lock();
        try {
            this.hardState = hardState;
        }finally {
            this.lock.unlock();
        }
    }

    public List<Entry> getEnts() {
        return ents;
    }
}
