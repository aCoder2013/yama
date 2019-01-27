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

import static com.song.yama.raft.utils.Utils.isEmptyHardState;
import static com.song.yama.raft.utils.Utils.isEmptySnap;

import com.song.yama.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.utils.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Ready encapsulates the entries and messages that are ready to read, be saved to stable storage, committed or sent to
 * other peers. All fields in Ready are read-only.
 */
@Data
public class Ready {


    /**
     * The current volatile state of a Node. SoftState will be nil if there is no update. It is not required to consume
     * or store SoftState.
     */
    private SoftState softState;

    /**
     * The current state of a Node to be saved to stable storage BEFORE Messages are sent. HardState will be equal to
     * empty state if there is no update.
     */
    private HardState hardState = HardState.getDefaultInstance();

    /**
     * ReadStates can be used for node to serve linearizable read requests locally when its applied index is greater
     * than the index in ReadState. Note that the readState will be returned when raft receives msgReadIndex. The
     * returned is only valid for the request that requested to read.
     */
    private List<ReadState> readStates;

    /**
     * Entries specifies entries to be saved to stable storage BEFORE Messages are sent.
     */
    private List<Entry> entries;


    /**
     * Snapshot specifies the snapshot to be saved to stable storage
     */
    private Snapshot snapshot;

    /**
     * Messages specifies outbound messages to be sent AFTER Entries are committed to stable storage. If it contains a
     * MsgSnap message, the application MUST report back to raft when the snapshot has been received or has failed by
     * calling ReportSnapshot.
     */
    private List<Entry> committedEntries;


    /**
     * Messages specifies outbound messages to be sent AFTER Entries are committed to stable storage. If it contains a
     * MsgSnap message, the application MUST report back to raft when the snapshot has been received or has failed by
     * calling ReportSnapshot.
     */
    private List<Message> messages;

    /**
     * MustSync indicates whether the HardState and Entries must be synchronously written to disk or if an asynchronous
     * write is permissible.
     */
    private boolean mustSync;

    public Ready(Raft raft, SoftState preSoftSt, HardState preHardSt) {
        this.entries = raft.getRaftLog().unstableEntries();
        this.committedEntries = raft.getRaftLog().nextEnts();
        if(CollectionUtils.isNotEmpty(raft.getMsgs())){
            this.messages = raft.getMsgs();
            raft.setMsgs(new ArrayList<>());
        }else {
            this.messages = Collections.emptyList();
        }
        SoftState softSt = raft.softState();
        if (!softSt.equals(preSoftSt)) {
            this.softState = softSt;
        }

        HardState hardSt = raft.hardState();
        if (!Utils.isHardStateEqual(hardSt, preHardSt)) {
            this.hardState = hardSt;

            if (CollectionUtils.isNotEmpty(this.committedEntries)) {
                Entry lastCommit = this.committedEntries.get(committedEntries.size() - 1);
                if (this.hardState.getCommit() > lastCommit.getIndex()) {
                    this.hardState = this.hardState.toBuilder().setCommit(lastCommit.getIndex()).build();
                }
            }
        }

        if (raft.getRaftLog().getUnstable().getSnapshot() != null) {
            this.snapshot = raft.getRaftLog().getUnstable().getSnapshot();
        }

        if (CollectionUtils.isNotEmpty(raft.getReadStates())) {
            this.readStates = raft.getReadStates();
            this.readStates.clear();
        }

        this.mustSync = mustSync(raft.hardState(), preHardSt, this.entries.size());
    }

    public boolean containsUpdates() {
        return this.softState != null || !isEmptyHardState(this.hardState) ||
            !isEmptySnap(this.snapshot) || CollectionUtils.isNotEmpty(this.entries) ||
            CollectionUtils.isNotEmpty(committedEntries) || CollectionUtils
            .isNotEmpty(this.messages)
            || CollectionUtils.isNotEmpty(this.readStates);
    }

    public static boolean mustSync(HardState st, HardState prevst, int entsnum) {
        return entsnum != 0 || st.getVote() != prevst.getVote() || st.getTerm() != prevst.getTerm();
    }
}
