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
import mu.KotlinLogging
import java.util.concurrent.ThreadLocalRandom

private val log = KotlinLogging.logger {}

class Network(val peers: Map<Long, StateMachine>, val storage: Map<Long, RaftStorage>, private var dropm: MutableMap<Connem, Float> = mutableMapOf(), private var ignorerm: MutableMap<RaftProtoBuf.MessageType, Boolean> = mutableMapOf()) {

    fun send(msgs: MutableList<RaftProtoBuf.Message>) {
        while (msgs.isNotEmpty()) {
            val newMsgs = mutableListOf<RaftProtoBuf.Message>()
            msgs.forEach { m: RaftProtoBuf.Message ->
                val p = this.peers[m.to]
                p?.run {
                    this.step(m)
                    val messages = this.readMessages()
                    newMsgs.addAll(filter(messages))
                } ?: throw RaftException("${m.to} doesn't exist")
            }
            msgs.clear()
            msgs.addAll(newMsgs)
        }
    }

    fun drop(from: Long, to: Long, perc: Float) {
        this.dropm.put(Connem(from, to), perc)
    }

    fun cut(one: Long, other: Long) {
        drop(one, other, 2.0F) // always drop
        drop(other, one, 2.0F) // always drop
    }

    fun isolate(id: Long) {
        for (i in 0 until peers.size) {
            val nid = i + 1
            if (nid.toLong() != id) {
                drop(id, nid.toLong(), 1.0F)
                drop(nid.toLong(), id, 1.0F)
            }
        }
    }

    fun recover() {
        this.dropm = mutableMapOf()
        this.ignorerm = mutableMapOf()
    }

    fun filter(msgs: List<RaftProtoBuf.Message>): List<RaftProtoBuf.Message> {
        val mm = mutableListOf<RaftProtoBuf.Message>()
        msgs.forEach { m: RaftProtoBuf.Message ->
            if (this.ignorerm.containsKey(m.type)) {
                return@forEach
            }
            if (m.type == RaftProtoBuf.MessageType.MsgHup) {
                // hups never go over the network, so don't drop them but panic
                throw RaftException("unexpected msgHup")
            } else {
                val perc = this.dropm.computeIfAbsent(Connem(m.from, m.to)) { 0f }
                val n = ThreadLocalRandom.current().nextFloat()
                if (n < perc) {
                    return@forEach
                }
            }
            log.info { "add message :${m.type},term:${m.term}" }
            mm.add(m)
        }
        return mm
    }

    fun ignore(t: RaftProtoBuf.MessageType) {
        this.ignorerm[t] = true
    }

    companion object {

        // initializes a network from peers.
        // A nil node will be replaced with a new StateMachine.
        // A StateMachine will get its k, id.
        // When using stateMachine, the address list is always [1, n].
        fun newNetwork(peers: List<StateMachine?>): Network = newNetworkWithConfig(peers)

        // newNetworkWithConfig is like newNetwork but calls the given func to
        // modify the configuration of any state machines it creates.
        fun newNetworkWithConfig(peers: List<StateMachine?>, preVote: Boolean = false): Network {
            val size = peers.size
            val peerAddrs = Utils.idsBySize(size)

            val npeers = mutableMapOf<Long, StateMachine>()
            val nstorage = mutableMapOf<Long, RaftStorage>()

            peers.forEachIndexed { j, p ->
                val id = peerAddrs[j]
                if (p == null) {
                    val memoryRaftStorage = MemoryRaftStorage()
                    val cfg = RaftTest.newTestConfig(id, peerAddrs, 10, 1, memoryRaftStorage)
                    cfg.preVote = preVote
                    nstorage[id] = memoryRaftStorage
                    val raft = Raft(cfg)
                    npeers[id] = StateMachine(raft)
                } else {
                    p.initial(id, peerAddrs)
                    npeers[id] = p
                }
            }

            return Network(npeers, nstorage)
        }
    }


}