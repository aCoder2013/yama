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

package com.song.yama.server.protocol.raft

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

class StateMachine(val raft: Raft?) {

    fun initial(id: Long, ids: List<Long>) {
        raft?.run {
            val learners = Maps.newHashMapWithExpectedSize<Long, Boolean>(this.learnerPrs.size)
            this.learnerPrs.keys.forEach {
                learners[it] = true
            }
            this.id = id
            this.prs = mutableMapOf()
            this.learnerPrs = mutableMapOf()

            ids.forEach {
                if (learners.containsKey(it)) {
                    this.learnerPrs[it] = Progress(isLearner = true)
                } else {
                    this.prs[it] = Progress()
                }
            }
            this.reset(this.term)
        }
    }

    fun step(message: RaftProtoBuf.Message) {
        if (raft != null) {
            raft.step(message)
        } else {
            log.info { "Skip step message :${message.type}" }
        }
    }

    fun readMessages(): List<RaftProtoBuf.Message> {
        return if (raft != null) {
            val msgs = raft.msgs
            raft.msgs = Lists.newArrayListWithExpectedSize(16)
            msgs
        } else {
            Lists.newArrayListWithExpectedSize(0)
        }
    }

    companion object {
        val NOP_STEPPER = StateMachine(null)
    }
}
