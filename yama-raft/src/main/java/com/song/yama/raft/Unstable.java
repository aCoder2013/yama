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

package com.song.yama.raft;


import com.song.yama.common.utils.Result;
import com.song.yama.raft.protobuf.RaftProtoBuf;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * unstable.entries[i] has raft log position i+unstable.offset. Note that unstable.offset may be
 * less than the highest log position in storage; this means that the next write to storage might
 * need to truncate the log before persisting unstable.entries.
 */
public class Unstable {

    private static final Logger log = LoggerFactory.getLogger(Unstable.class);

    /**
     * the incoming unstable snapshot, if any
     */
    private Snapshot snapshot;

    /**
     * all entries that have not yet been written to storage
     */
    private List<Entry> entries = new ArrayList<>();

    private long offset;

    public Unstable() {
    }

    public Unstable(long offset) {
        this.offset = offset;
    }

    /**
     * maybeFirstIndex returns the index of the first possible entry in entries if it has a
     * snapshot.
     */
    public Result<Long> maybeFirstIndex() {
        if (snapshot != null) {
            return Result.success(snapshot.getMetadata().getIndex() + 1);
        }
        return Result.fail(0L);
    }

    /**
     * maybeLastIndex returns the last index if it has at least one unstable entry or snapshot.
     */
    public Result<Long> maybeLastIndex() {
        if (CollectionUtils.isNotEmpty(this.entries)) {
            return Result.success(this.offset + this.entries.size() - 1);
        }

        if (this.snapshot != null) {
            return Result.success(this.snapshot.getMetadata().getIndex());
        }

        return Result.fail(0L);
    }

    /**
     * maybeTerm returns the term of the entry at index i, if there is any.
     */
    public Result<Long> maybeTerm(long i) {
        if (i < this.offset) {
            if (this.snapshot == null) {
                return Result.fail(0L);
            }

            if (this.snapshot.getMetadata().getIndex() == i) {
                return Result.success(this.snapshot.getMetadata().getTerm());
            }

            return Result.fail(0L);
        }

        Result<Long> result = maybeLastIndex();
        if (result.isFailure()) {
            return Result.fail(0L);
        }

        if (i > result.getData()) {
            return Result.fail(0L);
        }

        return Result.success(this.entries.get((int) (i - this.offset)).getTerm());
    }

    public void stableTo(long i, long t) {
        Result<Long> result = maybeTerm(i);
        if (result.isFailure()) {
            return;
        }

        // if i < offset, term is matched with the snapshot
        // only update the unstable entries if term is matched with
        // an unstable entry.
        if (result.getData() == t && i >= this.offset) {
            this.entries = this.entries.subList((int) (i + 1 - this.offset), this.entries.size());
            this.offset = i + 1;
            shrinkEntriesArray();
        }

    }

    /**
     * shrinkEntriesArray discards the underlying array used by the entries slice if most of it
     * isn't being used. This avoids holding references to a bunch of potentially large entries that
     * aren't needed anymore. Simply clearing the entries wouldn't be safe because clients might
     * still be using them.
     */
    public void shrinkEntriesArray() {
        // We replace the array if we're using less than half of the space in
        // it. This number is fairly arbitrary, chosen as an attempt to balance
        // memory usage vs number of allocations. It could probably be improved
        // with some focused tuning.
        //TODO:clean up resources
//        int lenMultiple = 2;
//        if(CollectionUtils.isEmpty(this.entries)){
//            this.entries = new ArrayList<>();
//        }else if(){
//
//        }
    }

    public void stableSnapTo(long i) {
        if (this.snapshot != null && this.snapshot.getMetadata().getIndex() == i) {
            this.snapshot = null;
        }
    }

    public void restore(RaftProtoBuf.Snapshot snapshot) {
        this.offset = snapshot.getMetadata().getIndex() + 1;
        this.entries = new ArrayList<>();
        this.snapshot = snapshot;
    }

    public void truncateAndAppend(List<RaftProtoBuf.Entry> ents) {
        long after = ents.get(0).getIndex();
        if (after == (offset + entries.size())) {
            // after is the next index in the u.entries
            // directly append
            this.entries.addAll(ents);
        } else if (after <= offset) {
            log.info("replace the unstable entries from index {}", after);
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            this.offset = after;
            entries = ents;
        } else {
            // truncate to after and copy to u.entries
            // then append
            log.info("truncate the unstable entries before index {}", after);
            this.entries = slice(this.offset, after);
            this.entries.addAll(ents);
        }
    }

    public List<Entry> slice(long lo, long hi) {
        mustCheckOutOfBounds(lo, hi);
        return new ArrayList<>(entries.subList((int) (lo - this.offset), (int) (hi - offset)));
    }

    public void mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            throw new IllegalArgumentException(
                String.format("invalid unstable.slice %d > %d", lo, hi));
        }

        long upper = this.offset + entries.size();
        if (lo < this.offset || hi > upper) {
            throw new IllegalArgumentException(String
                .format("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, this.offset, upper));
        }
    }

    public static Logger getLog() {
        return log;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
