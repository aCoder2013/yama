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

package com.song.yama.raft

import com.song.yama.raft.protobuf.RaftProtoBuf.HardState
import java.util.*

class Status {

    var id: Long? = null

    var hardState: HardState? = null

    var softState: SoftState? = null

    var applied: Long? = null

    var progress: Map<Long, Progress>? = null

    var leadTransferee: Long? = null

    companion object {

        fun getStatus(raft: Raft): Status {
            val status = Status()
            status.id = raft.id
            status.leadTransferee = raft.leadTransferee
            status.hardState = raft.hardState()
            status.softState = raft.softState()
            status.applied = raft.raftLog.applied
            if (status.softState!!.raftState == StateType.LEADER) {
                val progressMap = HashMap<Long, Progress>()
                raft.prs.forEach { key, value -> progressMap.put(key, value) }
                raft.learnerPrs.forEach { key, value -> progressMap.put(key, value) }
                status.progress = progressMap
            }
            return status
        }
    }
}
