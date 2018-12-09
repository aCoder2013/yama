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

import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf
import org.apache.commons.collections4.CollectionUtils

class Readonly(var option: ReadOnlyOption) {

    var pendingReadIndex = mutableMapOf<String, ReadIndexStatus>()

    var readIndexQueue = mutableListOf<String>()

    /**
     * addRequest adds a read only reuqest into readonly struct.
     * `index` is the commit index of the raft state machine when it received
     * the read only request.
     * `m` is the original read only request message from the local or remote node.
     */
    fun addRequest(index: Long, m: RaftProtoBuf.Message) {
        val ctx = String(m.entriesList[0].data.toByteArray())
        if (pendingReadIndex.containsKey(ctx)) {
            return
        }
        pendingReadIndex[ctx] = ReadIndexStatus(m, index, mutableSetOf())
        readIndexQueue.add(ctx)
    }

    /**
     * recvAck notifies the readonly struct that the raft state machine received
     * an acknowledgment of the heartbeat that attached with the read only request
     * context.
     */
    fun recvAck(m: RaftProtoBuf.Message): Int {
        val rs = pendingReadIndex[m.context.toStringUtf8()]
        if (rs == null) {
            return 0
        } else {
            rs.acks.add(m.from)
            // add one to include an ack from local node
            return rs.acks.size + 1
        }
    }

    /**
     * advance advances the read only request queue kept by the readonly struct.
     * It dequeues the requests until it finds the read only request that has
     * the same context as the given `m`.
     */
    fun advance(m: RaftProtoBuf.Message): List<ReadIndexStatus> {
        var i = 0
        var found = false
        val ctx = m.context.toStringUtf8()
        val rss = mutableListOf<ReadIndexStatus>()

        readIndexQueue.forEach {
            i++
            val rs = pendingReadIndex[it]
            if (rs == null) {
                throw RaftException("cannot find corresponding read state from pending map")
            } else {
                rss.add(rs)
                if (it == ctx) {
                    found = true
                    return@forEach
                }
            }
        }

        if (found) {
            this.readIndexQueue = readIndexQueue.subList(i, readIndexQueue.size).toMutableList()
            rss.forEach {
                pendingReadIndex.remove(it.req.getEntries(0).data.toStringUtf8())
            }

            return rss
        }
        return emptyList()
    }

    /**
    lastPendingRequestCtx returns the context of the last pending read only
    request in readonly struct.
     */
    fun lastPendingRequestCtx(): String {
        if (CollectionUtils.isEmpty(this.readIndexQueue)) {
            return ""
        }

        return this.readIndexQueue[readIndexQueue.size - 1]
    }

}
