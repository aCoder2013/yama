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

package com.song.yama.raft

import com.song.yama.raft.StateType.LEADER
import com.song.yama.raft.protobuf.RaftProtoBuf.MessageType
import com.song.yama.raft.utils.ProtoBufUtils.buildEntry
import com.song.yama.raft.utils.ProtoBufUtils.buildMessage
import com.song.yama.raft.utils.Utils
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Deterministic fault-injection scenarios on top of [Network] (message loss / partition).
 * Complements the large etcd-derived suite in [RaftTest] with asymmetric link failures.
 */
class RaftFaultInjectionTest {

    @Test
    fun asymmetricDropLeaderOutwardBlocksCommit() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val leader = nt.peers[1]!!.raft!!
        assertEquals(LEADER, leader.state)
        val committedBefore = leader.raftLog.committed

        // Leader -> followers path is broken; followers can still reach the leader.
        nt.drop(1, 2, 2.0f)
        nt.drop(1, 3, 2.0f)

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("blocked".toByteArray())))))

        assertEquals(committedBefore, leader.raftLog.committed, "without majority replication committed index must not advance")
        assertTrue(nt.peers[2]!!.raft!!.raftLog.lastIndex() <= committedBefore)
        assertTrue(nt.peers[3]!!.raft!!.raftLog.lastIndex() <= committedBefore)
    }

    @Test
    fun asymmetricDropRecoversAndLogConverges() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.drop(1, 2, 2.0f)
        nt.drop(1, 3, 2.0f)
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("during-partition".toByteArray())))))

        nt.recover()
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgBeat)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("after-heal".toByteArray())))))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgBeat)))

        val want = Utils.ltoa(nt.peers[1]!!.raft!!.raftLog)
        nt.peers.values.forEach { p ->
            val r = p.raft ?: return@forEach
            assertEquals(want, Utils.ltoa(r.raftLog))
        }
    }

    @Test
    fun minorityPartitionDoesNotElectLeader() {
        val nt = Network.newNetwork(mutableListOf(null, null, null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        assertEquals(LEADER, nt.peers[1]!!.raft!!.state)

        // Minority {4,5} cannot see {1,2,3}; pair still talks to each other but lacks quorum (need 3 of 5).
        val minority = listOf(4L, 5L)
        val majority = listOf(1L, 2L, 3L)
        for (a in minority) {
            for (b in majority) {
                nt.cut(a, b)
            }
        }

        nt.send(mutableListOf(buildMessage(4, 4, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(5, 5, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(4, 4, MessageType.MsgHup)))

        assertTrue(nt.peers[4]!!.raft!!.state != LEADER)
        assertTrue(nt.peers[5]!!.raft!!.state != LEADER)
        assertEquals(LEADER, nt.peers[1]!!.raft!!.state)
    }

    @Test
    fun leaderRejoinsAfterMajorityWentAhead() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("a".toByteArray())))))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgBeat)))

        // Old leader isolated; {2,3} form majority and move the log forward.
        nt.isolate(1)
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgProp, mutableListOf(buildEntry("b".toByteArray())))))
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgBeat)))

        nt.recover()
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgBeat)))
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgBeat)))

        val leaderCount = nt.peers.values.count { it.raft?.state == LEADER }
        assertEquals(1, leaderCount)

        val leaderRaft = nt.peers.values.mapNotNull { it.raft }.first { it.state == LEADER }
        val want = Utils.ltoa(leaderRaft.raftLog)
        nt.peers.values.forEach { p ->
            val r = p.raft ?: return@forEach
            assertEquals(want, Utils.ltoa(r.raftLog))
        }
    }
}
