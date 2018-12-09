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

package com.song.yama.server.protocol.raft;

import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.HardState;
import java.util.HashMap;
import java.util.Map;

public class Status {

    private Long id;

    private HardState hardState;

    private SoftState softState;

    private Long applied;

    private Map<Long, Progress> progress;

    private Long leadTransferee;

    public static Status getStatus(Raft raft) {
        Status status = new Status();
        status.setId(raft.getId());
        status.setLeadTransferee(raft.getLeadTransferee());
        status.setHardState(raft.hardState());
        status.setSoftState(raft.softState());
        status.setApplied(raft.getRaftLog().getApplied());
        if (status.getSoftState().getRaftState() == StateType.LEADER) {
            Map<Long, Progress> progressMap = new HashMap<>();
            raft.getPrs().forEach(progressMap::put);
            raft.getLearnerPrs().forEach(progressMap::put);
            status.setProgress(progressMap);
        }
        return status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public HardState getHardState() {
        return hardState;
    }

    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public SoftState getSoftState() {
        return softState;
    }

    public void setSoftState(SoftState softState) {
        this.softState = softState;
    }

    public Long getApplied() {
        return applied;
    }

    public void setApplied(Long applied) {
        this.applied = applied;
    }

    public Map<Long, Progress> getProgress() {
        return progress;
    }

    public void setProgress(Map<Long, Progress> progress) {
        this.progress = progress;
    }

    public Long getLeadTransferee() {
        return leadTransferee;
    }

    public void setLeadTransferee(Long leadTransferee) {
        this.leadTransferee = leadTransferee;
    }
}
