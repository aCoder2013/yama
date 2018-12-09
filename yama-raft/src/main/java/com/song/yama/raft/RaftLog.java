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
import com.song.yama.raft.exception.RaftException;
import com.song.yama.raft.protobuf.RaftProtoBuf;
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.utils.ErrorCode;
import com.song.yama.raft.utils.Utils;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RaftLog {

    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);

    /**
     * storage contains all stable entries since the last snapshot
     */
    private RaftStorage raftStorage;

    /**
     * unstable contains all unstable entries and snapshot. they will be saved into storage
     */
    private Unstable unstable;

    /**
     * committed is the highest log position that is known to be in stable storage on a quorum of nodes.
     */
    private long committed;

    /**
     * applied is the highest log position that the application has been instructed to apply to its state machine.
     * Invariant: applied <= committed
     */
    private long applied;

    private long maxMsgSize;

    public RaftLog(RaftStorage raftStorage) {
        this(raftStorage, Long.MAX_VALUE);
    }

    public RaftLog(RaftStorage raftStorage, long maxMsgSize) {
        this.raftStorage = raftStorage;
        this.maxMsgSize = maxMsgSize;
        long firstIndex = raftStorage.firstIndex();
        long lastIndex = raftStorage.lastIndex();

        this.unstable = new Unstable();
        this.unstable.setOffset(lastIndex + 1);
        this.committed = firstIndex - 1;
        this.applied = firstIndex - 1;
    }

    /**
     * maybeAppend returns (0, false) if the entries cannot be appended. Otherwise, it returns (last index of new
     * entries, true).
     */
    public Result<Long> maybeAppend(long index, long logTerm, long committed,
        List<RaftProtoBuf.Entry> ents) {
        if (matchTerm(index, logTerm)) {
            long lastnewi = index + CollectionUtils.size(ents);
            long ci = findConflict(ents);
            if (ci == 0) {

            } else if (ci <= this.committed) {
                String message = String
                    .format("entry %d conflict with committed entry [committed(%d)]", ci, this.committed);
                log.warn(message);
                throw new RaftException(message);
            } else {
                long offset = index + 1;
                append(ents.subList((int) (ci - offset), ents.size()));
            }
            commitTo(Math.min(committed, lastnewi));
            return Result.success(lastnewi);
        }
        return Result.fail(0L);
    }

    /**
     * / findConflict finds the index of the conflict. It returns the first pair of conflicting entries between the
     * existing entries and the given entries, if there are any. If there is no conflicting entries, and the existing
     * entries contains all the given entries, zero will be returned. If there is no conflicting entries, but the given
     * entries contains new entries, the index of the first new entry will be returned. An entry is considered to be
     * conflicting if it has the same index but a different term. The first entry MUST have an index equal to the
     * argument 'from'. The index of the given entries MUST be continuously increasing.
     */
    public long findConflict(List<RaftProtoBuf.Entry> ents) {
        if (ents != null && ents.size() > 0) {
            for (Entry entry : ents) {
                if (!matchTerm(entry.getIndex(), entry.getTerm())) {
                    if (entry.getIndex() <= lastIndex()) {
                        log.info(String.format(
                            "found conflict at index %d [existing term: %d, conflicting term: %d]",
                            entry.getIndex(),
                            this.zeroTermOnErrCompacted(this.term(entry.getIndex()).getData(), null),
                            entry.getTerm()));
                    }
                    return entry.getIndex();
                }
            }
        }
        return 0;
    }

    public List<RaftProtoBuf.Entry> unstableEntries() {
        if (CollectionUtils.isEmpty(this.unstable.getEntries())) {
            return Collections.emptyList();
        }

        return this.unstable.getEntries();
    }

    /**
     * nextEnts returns all the available entries for execution. If applied is smaller than the index of snapshot, it
     * returns all committed entries after the index of snapshot.
     */
    public List<Entry> nextEnts() {
        long off = Math.max(this.applied + 1, firstIndex());
        if (this.committed + 1 > off) {
            Result<List<Entry>> result = slice(off, this.committed + 1, maxMsgSize);
            if (result.isFailure()) {
                throw new IllegalStateException(
                    String.format("unexpected error when getting unapplied entries (%s)",
                        ErrorCode.getByCode(result.getCode()).getDesc()));
            }
            return result.getData();
        }
        return Collections.emptyList();
    }


    /**
     * hasNextEnts returns if there is any available entries for execution. This is a fast check without heavy
     * raftLog.slice() in raftLog.nextEnts().
     */
    public boolean hasNextEnts() {
        long off = Math.max(this.applied + 1, firstIndex());
        return this.committed + 1 > off;
    }

    public Result<Snapshot> snapshot() {
        if (this.unstable.getSnapshot() != null) {
            return Result.success(this.unstable.getSnapshot());
        }
        return this.raftStorage.snapshot();
    }

    public void commitTo(long toCommit) {
        // never decrease commit
        if (this.committed < toCommit) {
            if (lastIndex() < toCommit) {
                throw new IllegalStateException(String.format(
                    "tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?",
                    toCommit, lastIndex()));
            }

            this.committed = toCommit;
        }
    }

    public void appliedTo(long i) {
        if (i == 0) {
            return;
        }
        if (committed < i || i < applied) {
            throw new IllegalStateException(String
                .format("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i,
                    this.applied, this.committed));
        }

        applied = i;
    }


    public void stableTo(long i, long t) {
        this.unstable.stableTo(i, t);
    }

    public void stableSnapTo(long i) {
        this.unstable.stableSnapTo(i);
    }

    public long lastTerm() {
        Result<Long> result = term(lastIndex());
        if (result.isSuccess()) {
            return result.getData();
        }
        throw new RaftException("Unexpected error when getting the last term : " + result.getCode());
    }

    public long append(List<Entry> entries) {
        if (CollectionUtils.isEmpty(entries)) {
            return lastIndex();
        }

        long after = entries.get(0).getIndex() - 1;
        if (after < committed) {
            throw new IllegalStateException(
                String.format("after(%d) is out of range [committed(%d)]", after, this.committed));
        }

        this.unstable.truncateAndAppend(entries);
        return lastIndex();
    }

    public long firstIndex() {
        Result<Long> result = this.unstable.maybeFirstIndex();
        if (result.isSuccess()) {
            return result.getData();
        }

        return this.raftStorage.firstIndex();
    }

    public long lastIndex() {
        Result<Long> result = unstable.maybeLastIndex();
        if (result.isSuccess()) {
            return result.getData();
        }

        return raftStorage.lastIndex();
    }

    public Result<Long> term(long i) {
        // the valid term range is [index of dummy entry, last index]
        long dummyIndex = firstIndex() - 1;
        if (i < dummyIndex || i > lastIndex()) {
            return Result.success(0L);
        }

        Result<Long> result = this.unstable.maybeTerm(i);
        if (result.isSuccess()) {
            return result;
        }

        Result<Long> termResult = this.raftStorage.term(i);
        if (termResult.isSuccess()) {
            return termResult;
        }

        if (result.getCode() == ErrorCode.ErrCompacted.getCode()
            || result.getCode() == ErrorCode.ErrUnavailable.getCode()) {
            return Result.success(0L);
        }

        throw new IllegalArgumentException("Unexpected error" + ErrorCode.getByCode(result.getCode()).getDesc());
    }

    public Result<List<Entry>> entries(long i, long maxSize) {
        if (i > lastIndex()) {
            return Result.success(Collections.emptyList());
        }

        return slice(i, lastIndex() + 1, maxSize);
    }

    public List<Entry> allEntries() {
        Result<List<Entry>> result = entries(firstIndex(), Long.MAX_VALUE);
        if (result.isSuccess()) {
            return result.getData();
        }

        if (result.getCode() == ErrorCode.ErrCompacted
            .getCode()) {
            //TODO:try again if there was a racing compaction
            return allEntries();
        }

        //FIXME:handle error
        throw new IllegalStateException(ErrorCode.getByCode(result.getCode()).getDesc());
    }

    /**
     * isUpToDate determines if the given (lastIndex,term) log is more up-to-date by comparing the index and term of the
     * last entries in the existing logs. If the logs have last entries with different terms, then the log with the
     * later term is more up-to-date. If the logs end with the same term, then whichever log has the larger lastIndex is
     * more up-to-date. If the logs are the same, the given log is up-to-date.
     */
    public boolean isUpToDate(long lasti, long term) {
        return term > lastTerm() || (term == lastTerm() && lasti >= lastIndex());
    }

    public boolean matchTerm(long i, long term) {
        try {
            Result<Long> result = term(i);
            return result.isSuccess() && result.getData() == term;
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            return false;
        }
    }


    public boolean maybeCommit(long maxIndex, long term) {
        if (maxIndex > this.committed && zeroTermOnErrCompacted(term(maxIndex).getData(), null) == term) {
            commitTo(maxIndex);
            return true;
        }
        return false;
    }

    public void restore(RaftProtoBuf.Snapshot snapshot) {
        log.info(String.format("log [%s] starts to restore snapshot [index: %d, term: %d]", this,
            snapshot.getMetadata().getIndex(), snapshot.getMetadata().getTerm()));
        this.committed = snapshot.getMetadata().getIndex();
        this.unstable.restore(snapshot);
    }

    public Result<List<Entry>> slice(long lo, long hi, long maxSize) {
        ErrorCode errorCode = mustCheckOutOfBounds(lo, hi);
        if (errorCode != null) {
            return Result.fail(errorCode.getCode(), errorCode.getDesc(), Collections.emptyList());
        }

        if (lo == hi) {
            return Result.success(Collections.emptyList());
        }

        List<Entry> ents = null;
        if (lo < this.unstable.getOffset()) {
            Result<List<Entry>> result = raftStorage
                .entries(lo, Math.min(hi, this.unstable.getOffset()), maxSize);
            if (result.isFailure()) {
                ErrorCode code = ErrorCode.getByCode(result.getCode());
                if (code == ErrorCode.ErrCompacted) {
                    return Result.fail(code.getCode(), code.getDesc(), Collections.emptyList());
                } else if (code == ErrorCode.ErrUnavailable) {
                    throw new IllegalStateException(String
                        .format("entries[%d:%d) is unavailable from storage", lo,
                            Math.min(hi, this.unstable.getOffset())));
                } else {
                    throw new IllegalStateException(code.getDesc());
                }
            }

            if (result.getData().size() < (Math.min(hi, this.unstable.getOffset()) - lo)) {
                return Result.success(result.getData());
            }

            ents = result.getData();
        }

        if (hi > this.unstable.getOffset()) {
            List<Entry> entries = this.unstable.slice(Math.max(lo, this.unstable.getOffset()), hi);
            if (CollectionUtils.isNotEmpty(ents)) {
                ents.addAll(entries);
            } else {
                ents = entries;
            }
        }

        return Result.success(Utils.limitSize(ents, maxSize));
    }

    // l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
    public ErrorCode mustCheckOutOfBounds(long lo, long hi) {
        if (lo > hi) {
            throw new IllegalStateException(String.format("invalid slice %d > %d", lo, hi));
        }

        long fi = firstIndex();

        if (lo < fi) {
            return ErrorCode.ErrCompacted;
        }

        long length = lastIndex() + 1 - fi;
        if (lo < fi || hi > fi + length) {
            throw new IllegalStateException(
                String.format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()));
        }

        return null;
    }

    public long zeroTermOnErrCompacted(Long t, ErrorCode errorCode) {
        if (t == null) {
            log.debug("t is null , change it into zero");
            t = 0L;
        }
        if (errorCode == null) {
            return t;
        }

        if (errorCode == ErrorCode.ErrCompacted) {
            return 0;
        }

        throw new IllegalStateException(
            String.format("unexpected error (%d)", errorCode.getCode()));
    }

    public RaftStorage getRaftStorage() {
        return raftStorage;
    }

    public void setRaftStorage(RaftStorage raftStorage) {
        this.raftStorage = raftStorage;
    }

    public Unstable getUnstable() {
        return unstable;
    }

    public void setUnstable(Unstable unstable) {
        this.unstable = unstable;
    }

    public long getCommitted() {
        return committed;
    }

    public void setCommitted(long committed) {
        this.committed = committed;
    }

    public long getApplied() {
        return applied;
    }

    public void setApplied(long applied) {
        this.applied = applied;
    }

    public long getMaxMsgSize() {
        return maxMsgSize;
    }

    public void setMaxMsgSize(long maxMsgSize) {
        this.maxMsgSize = maxMsgSize;
    }
}
