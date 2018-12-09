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
import com.google.protobuf.ByteString
import com.song.yama.common.utils.Result
import com.song.yama.server.protocol.raft.Progress.ProgressStateType
import com.song.yama.server.protocol.raft.Progress.ProgressStateType.*
import com.song.yama.server.protocol.raft.StateType.*
import com.song.yama.server.protocol.raft.exception.RaftException
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.*
import com.song.yama.server.protocol.raft.utils.ErrorCode
import com.song.yama.server.protocol.raft.utils.ProtoBufUtils
import com.song.yama.server.protocol.raft.utils.ProtoBufUtils.buildEntry
import com.song.yama.server.protocol.raft.utils.ProtoBufUtils.buildMessage
import com.song.yama.server.protocol.raft.utils.Utils
import mu.KotlinLogging
import org.junit.Test
import org.mockito.Mockito.*
import java.util.*
import kotlin.math.min
import kotlin.test.*

private val log = KotlinLogging.logger {}

class RaftTest {

    @Test
    fun testProgressBecomeProbe() {
        val match: Long = 1

        class Item(var progress: Progress, var wnext: Long)

        val tests = ArrayList<Item>()
        tests.add(Item(Progress(match, 5, ProgressStateReplicate, Inflight(256)), 2))
        tests.add(Item(Progress(match, 5, ProgressStateSnapshot, 10, Inflight(256)), 11))
        tests.add(Item(Progress(match, 5, ProgressStateSnapshot, 0, Inflight(256)), 2))

        for (i in tests.indices) {
            val item = tests[i]
            item.progress.becomeProbe()

            if (item.progress.state != ProgressStateProbe) {
                throw RaftException(String.format("#%d: state = %s, want %s", i, item.progress.state,
                        ProgressStateProbe))
            }

            if (item.progress.match != match) {
                throw RaftException(String.format("#%d: match = %d, want %d", i, item.progress.match, match))
            }

            if (item.progress.next != item.wnext) {
                throw RaftException(
                        String.format("#%d: next = %d, want %d", i, item.progress.next, item.wnext))
            }
        }
    }

    @Test
    fun testProgressBecomeReplicate() {
        val p = Progress(1, 5, ProgressStateProbe, Inflight(256))
        p.becomeReplicate()


        if (p.state != ProgressStateReplicate) {
            throw RaftException(String.format("state = %s, want %s", p.state,
                    ProgressStateProbe))
        }

        if (p.match != 1L) {
            throw RaftException(String.format("match = %d, want %d", p.match, 1))
        }

        val w = p.match + 1
        if (p.next != w) {
            throw RaftException(
                    String.format("next = %d, want %d", p.next, w))
        }
    }

    @Test
    fun testProgressBecomeSnapshot() {
        val p = Progress(1, 5, ProgressStateProbe, Inflight(256))
        p.becomeSnapshot(10)


        if (p.state != ProgressStateSnapshot) {
            throw RaftException(String.format("state = %s, want %s", p.state,
                    ProgressStateProbe))
        }

        if (p.match != 1L) {
            throw RaftException(String.format("match = %d, want %d", p.match, 1))
        }

        if (p.pendingSnapshot != 10L) {
            throw RaftException(
                    String.format("pendingSnapshot = %d, want %d", p.next, 10))
        }
    }

    @Test
    fun testProgressUpdate() {
        val prevM = 3L
        val prevN = 5L

        class Item(var update: Long, var wm: Long, var wn: Long, var wok: Boolean)

        val list = mutableListOf(
                Item(prevM - 1, prevM, prevN, false),
                Item(prevM, prevM, prevN, false),
                Item(prevM + 1, prevM + 1, prevN, true),
                Item(prevM + 2, prevM + 2, prevN + 1, true))

        list.forEachIndexed { i, tt ->
            val p = Progress(prevM, prevN)
            val ok = p.maybeUpdate(tt.update)
            if (ok != tt.wok) {
                throw RaftException(String.format("#%d: ok= %v, want %v", i, ok, tt.wok))
            }

            if (p.match != tt.wm) {
                throw RaftException(String.format("#%d: match= %d, want %d", i, p.match, tt.wm))
            }

            if (p.next != tt.wn) {
                throw RaftException(String.format("#%d: next= %d, want %d", i, p.next, tt.wn))
            }
        }
    }

    @Test
    fun testProgressMaybeDecr() {
        class Item(var state: ProgressStateType, var m: Long, var n: Long, var reject: Long, var last: Long, var w: Boolean, var wn: Long)

        val tests = mutableListOf(
                // state replicate and rejected is not greater than match
                Item(ProgressStateReplicate, 5, 10, 5, 5, false, 10L),
                // state replicate and rejected is not greater than mat
                Item(ProgressStateReplicate, 5, 10, 4, 4, false, 10),
                // state replicate and rejected is greater than match
                // directly decrease to match+1
                Item(ProgressStateReplicate, 5, 10, 9, 9, true, 6),
                // state replicate and rejected is greater than match
                // directly decrease to match+1
                Item(ProgressStateProbe, 0, 0, 0, 0, false, 0),
                // next-1 != rejected is always false
                Item(ProgressStateProbe, 0, 10, 5, 5, false, 10),
                // next>1 = decremented by 1
                Item(ProgressStateProbe, 0, 10, 9, 9, true, 9),
                // next>1 = decremented by 1
                Item(ProgressStateProbe, 0, 2, 1, 1, true, 1),
                // next<=1 = reset to 1
                Item(ProgressStateProbe, 0, 1, 0, 0, true, 1),
                // decrease to min(rejected, last+1)
                Item(ProgressStateProbe, 0, 10, 9, 2, true, 3),
                // rejected < 1, reset to 1
                Item(ProgressStateProbe, 0, 10, 9, 0, true, 1))

        tests.forEachIndexed { i, tt ->
            val p = Progress(tt.state, tt.m, tt.n)

            val g = p.maybeDecrTo(tt.reject, tt.last)
            if (g != tt.w) {
                throw RaftException(String.format("#%d: maybeDecrTo= %t, want %t", i, g, tt.w))
            }

            val gm = p.match

            if (gm != tt.m) {
                throw RaftException(String.format("#%d: match= %d, want %d", i, gm, tt.m))
            }

            val gn = p.next

            if (gn != tt.wn) {
                throw RaftException(String.format("#%d: next= %d, want %d", i, gn, tt.wn))
            }
        }
    }


    @Test
    fun testProgressIsPaused() {
        class Item(val state: ProgressStateType, val paused: Boolean, var w: Boolean)

        val tests = mutableListOf<Item>(
                Item(ProgressStateProbe, false, false),
                Item(ProgressStateProbe, true, true),
                Item(ProgressStateReplicate, false, false),
                Item(ProgressStateReplicate, true, false),
                Item(ProgressStateSnapshot, false, true),
                Item(ProgressStateSnapshot, true, true))

        tests.forEachIndexed { i, tt ->
            val p = Progress(tt.state, tt.paused, Inflight(256))

            val g = p.isPaused()
            if (g != tt.w) {
                throw RaftException(String.format("#%d: paused= %b, want %b", i, g, tt.w))
            }
        }
    }


    /**
     * TestProgressResume ensures that progress.maybeUpdate and progress.maybeDecrTo
     * will reset progress.paused.
     */
    @Test
    fun testProgressResume() {
        val p = Progress(next = 2, paused = true)

        p.maybeDecrTo(1, 1)
        if (p.paused) {
            throw RaftException(String.format("paused= %v, want false", p.paused))
        }

        p.paused = true
        p.maybeUpdate(2)
        if (p.paused) {
            throw RaftException(String.format("paused= %v, want false", p.paused))
        }
    }

    @Test
    fun testProgressResumeByHeartbeatResp() {
        val raft = newTestRaft(1, mutableListOf(1, 2), 5, 1, MemoryRaftStorage())
        raft.becomeCandidate()
        raft.becomeLeader()
        raft.prs[2]!!.paused = true
        raft.step(Message.newBuilder()
                .setFrom(1)
                .setTo(1)
                .setType(MessageType.MsgBeat)
                .build())

        if (!raft.prs[2]!!.paused) {
            throw RaftException(String.format("paused = %v, want true", raft.prs[2]!!.paused))
        }

        raft.prs[2]!!.becomeReplicate()

        raft.step(Message.newBuilder()
                .setFrom(2)
                .setTo(1)
                .setType(MessageType.MsgHeartbeatResp)
                .build())

        if (raft.prs[2]!!.paused) {
            throw RaftException(String.format("paused = %b, want false", raft.prs[2]!!.paused))
        }
    }

    @Test
    fun testProgressPaused() {
        val raft = newTestRaft(1, mutableListOf(1, 2), 5, 1, MemoryRaftStorage())

        raft.becomeCandidate()
        raft.becomeLeader()
        raft.step(Message.newBuilder()
                .setFrom(1)
                .setTo(1)
                .setType(MessageType.MsgProp)
                .addEntries(Entry.newBuilder().setData(ByteString.copyFrom("somedata", "UTF-8")).build())
                .build())

        raft.step(Message.newBuilder()
                .setFrom(1)
                .setTo(1)
                .setType(MessageType.MsgProp)
                .addEntries(Entry.newBuilder().setData(ByteString.copyFrom("somedata", "UTF-8")).build())
                .build())

        raft.step(Message.newBuilder()
                .setFrom(1)
                .setTo(1)
                .setType(MessageType.MsgProp)
                .addEntries(Entry.newBuilder().setData(ByteString.copyFrom("somedata", "UTF-8")).build())
                .build())

        val ms = Companion.readMessages(raft)
        if (ms.size != 1) {
            throw RaftException(String.format("len(ms) = %d, want 1", ms.size))
        }
    }

    @Test
    fun testProgressFlowControl() {
        val cfg = newTestConfig(1, mutableListOf(1, 2), 5, 1, MemoryRaftStorage())
        cfg.maxInflightMsgs = 3
        cfg.maxSizePerMsg = 2048
        val r = Raft(cfg)
        r.becomeCandidate()
        r.becomeLeader()

        // Throw away all the messages relating to the initial election.
        Companion.readMessages(r)

        // While node 2 is in probe state, propose a bunch of entries.
        r.prs[2]!!.becomeProbe()
        val blob = "a".repeat(1000).toByteArray()
        for (i in 0 until 10) {
            r.step(Message.newBuilder()
                    .setFrom(1)
                    .setTo(1)
                    .setType(MessageType.MsgProp)
                    .addAllEntries(mutableListOf(Entry.newBuilder().setData(ByteString.copyFrom(blob)).build()))
                    .build())
        }

        var ms = Companion.readMessages(r)
        // First append has two entries: the empty entry to confirm the
        // election, and the first proposal (only one proposal gets sent
        // because we're in probe state).
        if (ms.size != 1 || ms[0].type != MessageType.MsgApp) {
            throw RaftException("expected 1 MsgApp, got : $ms")
        }

        assertEquals(2, ms[0].entriesCount)
        if (ms[0].getEntries(0).data.size() != 0 || ms[0].getEntries(1).data.size() != 1000) {
            throw RaftException("unexpected entry sizes: ${ms[0].entriesList}")
        }

        // When this append is acked, we change to replicate state and can
        // send multiple messages at once.
        r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgAppResp).setIndex(ms[0].getEntries(1).index).build())

        ms = Companion.readMessages(r)

        assertEquals(3, ms.size)
        ms.forEachIndexed { i, m ->
            assertEquals(MessageType.MsgApp, m.type)
            assertEquals(2, m.entriesCount)
        }

        // Ack all three of those messages together and get the last two
        // messages (containing three entries).
        r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgAppResp).setIndex(ms[2].getEntries(1).index).build())
        ms = Companion.readMessages(r)
        assertEquals(2, ms.size)
        ms.forEachIndexed { i, m ->
            assertEquals(MessageType.MsgApp, m.type)
        }

        assertEquals(2, ms[0].entriesCount)
        assertEquals(1, ms[1].entriesCount)
    }

    @Test
    fun testLeaderElection() {
        testLeaderElectionWithConfig(false)
    }

    @Test
    fun testLeaderElectionPreVote() {
        testLeaderElectionWithConfig(true)
    }

    private fun testLeaderElectionWithConfig(preVote: Boolean) {
        class Item(val network: Network, val stateType: StateType, val expTerm: Long)

        var candState = CANDIDATE
        var candTerm = 1L
        if (preVote) {
            candState = PRE_CANDIDATE
            // In pre-vote mode, an election that fails to complete
            // leaves the node in pre-candidate state without advancing
            // the term.
            candTerm = 0
        }
        val tests = mutableListOf(
                Item(Network.newNetworkWithConfig(mutableListOf(null, null, null), preVote), LEADER, 1),
                Item(Network.newNetworkWithConfig(mutableListOf(null, null, StateMachine.NOP_STEPPER), preVote), LEADER, 1),
                Item(Network.newNetworkWithConfig(mutableListOf(null, StateMachine.NOP_STEPPER, StateMachine.NOP_STEPPER), preVote), candState, candTerm),
                Item(Network.newNetworkWithConfig(mutableListOf(null, StateMachine.NOP_STEPPER, StateMachine.NOP_STEPPER, null), preVote), candState, candTerm),
                Item(Network.newNetworkWithConfig(mutableListOf(null, StateMachine.NOP_STEPPER, StateMachine.NOP_STEPPER, null, null), preVote), LEADER, 1),
                // three logs further along than 0, but in the same term so rejections
                // are returned instead of the votes being ignored.
                Item(Network.newNetworkWithConfig(mutableListOf(null,
                        entsWithConfig(mutableListOf(1), preVote),
                        entsWithConfig(mutableListOf(1), preVote),
                        entsWithConfig(mutableListOf(1, 1), preVote),
                        null), preVote), FOLLOWER, 1))

        tests.forEachIndexed { i, tt ->
            tt.network.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build()))
            val sm = tt.network.peers[1]
            assertEquals(tt.stateType, sm!!.raft!!.state, "$i")
            assertEquals(tt.expTerm, sm.raft!!.term, "index:$i , term should be equal")
        }
    }

    /**
     * TestLearnerElectionTimeout verfies that the leader should not start election even
     * when times out.
     */
    @Test
    fun testLearnerElectionTimeout() {
        val n1 = newTestLearnerRaft(1, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())
        val n2 = newTestLearnerRaft(2, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())

        n1.becomeFollower(1, 0)
        n2.becomeFollower(1, 0)

        // n2 is learner. Learner should not start election even when times out.
        setRandomizedElectionTimeout(n2, n2.electionTimeout)
        for (i in 0 until n2.electionTimeout) {
            n2.tick()
        }

        assertEquals(FOLLOWER, n2.state)
    }

    // TestLearnerPromotion verifies that the learner should not election until
    // it is promoted to a normal peer.
    @Test
    fun testLearnerPromotion() {
        val n1 = newTestLearnerRaft(1, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())
        val n2 = newTestLearnerRaft(2, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())

        n1.becomeFollower(1, 0)
        n2.becomeFollower(1, 0)

        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2)))

        assertNotEquals(LEADER, n1.state)

        // n1 should become leader
        setRandomizedElectionTimeout(n1, n1.electionTimeout)
        for (i in 0 until n1.electionTimeout) {
            n1.tick()
        }

        assertEquals(LEADER, n1.state)
        assertEquals(FOLLOWER, n2.state)

        nt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgBeat).build()))

        n1.addNode(2)
        n2.addNode(2)

        if (n2.isLearner) {
            throw RaftException("peer 2 is learner, want not")
        }

        // n2 start election, should become leader
        setRandomizedElectionTimeout(n2, n2.electionTimeout)
        for (i in 0 until n2.electionTimeout) {
            n2.tick()
        }

        nt.send(mutableListOf(Message.newBuilder().setFrom(2).setTo(2).setType(MessageType.MsgBeat).build()))

        assertEquals(FOLLOWER, n1.state)
        assertEquals(LEADER, n2.state)

    }

    @Test
    fun testLearnerCannotVote() {
        val n2 = newTestLearnerRaft(2, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())

        n2.becomeFollower(1, 0)

        n2.step(Message.newBuilder().setFrom(1).setTo(2).setTerm(2).setType(MessageType.MsgVote).setLogTerm(11).setIndex(11).build())

        assertEquals(n2.msgs.size, 0, "expect learner not to vote, but received ${n2.msgs.size} messages")
    }

    @Test
    fun testLeaderCycle() {
        testLeaderCycle(false)
    }

    @Test
    fun testLeaderCyclePreVote() {
        testLeaderCycle(true)
    }

    // testLeaderCycle verifies that each node in a cluster can campaign
    // and be elected in turn. This ensures that elections (including
    // pre-vote) work when not starting from a clean slate (as they do in
    // TestLeaderElection)
    fun testLeaderCycle(preVote: Boolean) {
        val n = Network.newNetworkWithConfig(mutableListOf(null, null, null), preVote)
        for (campaignerID: Long in 1L..3L) {
            n.send(mutableListOf(Message.newBuilder().setFrom(campaignerID).setTo(campaignerID).setType(MessageType.MsgHup).build()))

            n.peers.forEach { _, sm ->
                val raft = sm.raft!!
                if (raft.id == campaignerID && raft.state != LEADER) {
                    fail("preVote=$preVote: campaigning node ${raft.id} state = ${raft.state}, want StateLeader")
                } else if (raft.id != campaignerID && raft.state != FOLLOWER) {
                    fail("preVote=$preVote: after campaign of node $campaignerID,node ${raft.id} had state = ${raft.state}, want StateFollower")
                }
            }
        }
    }

    // TestLeaderElectionOverwriteNewerLogs tests a scenario in which a
    // newly-elected leader does *not* have the newest (i.e. highest term)
    // log entries, and must overwrite higher-term log entries with
    // lower-term ones.
    @Test
    fun testLeaderElectionOverwriteNewerLogs() {
        testLeaderElectionOverwriteNewerLogs(false)
    }

    @Test
    fun testLeaderElectionOverwriteNewerLogsPreVote() {
        testLeaderElectionOverwriteNewerLogs(true)
    }

    fun testLeaderElectionOverwriteNewerLogs(preVote: Boolean) {
        // This network represents the results of the following sequence of
        // events:
        // - Node 1 won the election in term 1.
        // - Node 1 replicated a log entry to node 2 but died before sending
        //   it to other nodes.
        // - Node 3 won the second election in term 2.
        // - Node 3 wrote an entry to its logs but died without sending it
        //   to any other nodes.
        //
        // At this point, nodes 1, 2, and 3 all have uncommitted entries in
        // their logs and could win an election at term 3. The winner's log
        // entry overwrites the losers'. (TestLeaderSyncFollowerLog tests
        // the case where older log entries are overwritten, so this test
        // focuses on the case where the newer entries are lost).
        val n = Network.newNetworkWithConfig(mutableListOf(
                entsWithConfig(mutableListOf(1), preVote),  // Node 1: Won first election
                entsWithConfig(mutableListOf(1), preVote),  // Node 2: Got logs from node 1
                entsWithConfig(mutableListOf(2), preVote),// Node 3: Won second election
                votedWithConfig(3, 2, preVote), // Node 4: Voted but didn't get logs
                votedWithConfig(3, 2, preVote)), // Node 5: Voted but didn't get logs
                preVote)

        // Node 1 campaigns. The election fails because a quorum of nodes
        // know about the election that already happened at term 2. Node 1's
        // term is pushed ahead to 2.
        n.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build()))
        val sm1 = n.peers[1]!!
        assertEquals(FOLLOWER, sm1.raft!!.state)
        assertEquals(2, sm1.raft.term)

        // Node 1 campaigns again with a higher term. This time it succeeds.
        n.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build()))
        assertEquals(LEADER, sm1.raft.state)
        assertEquals(3, sm1.raft.term)

        // Now all nodes agree on a log entry with term 1 at index 1 (and
        // term 3 at index 2).
        n.peers.forEach { _, sm ->
            val raft = sm.raft!!
            val entries = raft.raftLog.allEntries()
            assertEquals(2, entries.size)
            assertEquals(1, entries[0].term)
            assertEquals(3, entries[1].term)
        }
    }

    @Test
    fun testVoteFromAnyState() {
        testVoteFromAnyState(MessageType.MsgVote)
    }

    @Test
    fun testPreVoteFromAnyState() {
        testVoteFromAnyState(MessageType.MsgPreVote)
    }

    fun testVoteFromAnyState(mt: RaftProtoBuf.MessageType) {
        StateType.values().forEach { state: StateType ->
            val r = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
            r.term = 1

            when (state) {
                FOLLOWER -> {
                    val t = r.term
                    r.becomeFollower(t, 3)
                }
                PRE_CANDIDATE -> {
                    r.becomePreCandidate()
                }
                CANDIDATE -> {
                    r.becomeCandidate()
                }
                LEADER -> {
                    r.becomeCandidate()
                    r.becomeLeader()
                }
            }

            // Note that setting our state above may have advanced r.term
            // past its initial value.
            val origTerm = r.term
            val newTerm = r.term + 1

            val msg = Message.newBuilder()
                    .setFrom(2)
                    .setTo(1)
                    .setType(mt)
                    .setTerm(newTerm)
                    .setLogTerm(newTerm)
                    .setIndex(42)
                    .build()

            r.step(msg)

            if (r.msgs.size != 1) {
                fail("$mt,$state: ${r.msgs.size} response messages, want 1: ${r.msgs}")
            } else {
                val resp = r.msgs[0]
                assertEquals(Utils.voteRespMsgType(mt), resp.type)
                assertFalse { resp.reject }
            }

            // If this was a real vote, we reset our state and term.
            if (mt == MessageType.MsgVote) {
                assertEquals(FOLLOWER, r.state)
                assertEquals(newTerm, r.term)
                assertEquals(2, r.vote)
            } else {
                // In a prevote, nothing changes.
                assertEquals(state, r.state)
                assertEquals(origTerm, r.term)
                // if st == StateFollower or StatePreCandidate, r hasn't voted yet.
                // In StateCandidate or StateLeader, it's voted for itself.
                if (r.vote != 0L && r.vote != 1L) {
                    fail("$mt,$state: vote ${r.vote}, want 0 or 1")
                }
            }

        }
    }

    @Test
    fun testLogReplication() {
        class Item(val network: Network, val msgs: MutableList<RaftProtoBuf.Message>, val wcommited: Long)

        val tests = mutableListOf(
                Item(Network.newNetwork(mutableListOf(null, null, null)),
                        mutableListOf(Message.newBuilder().setFrom(1).setTo(1)
                                .setType(MessageType.MsgProp)
                                .addAllEntries(mutableListOf(ProtoBufUtils.buildEntry("somedata".toByteArray()))).build()),
                        2),

                Item(Network.newNetwork(mutableListOf(null, null, null)), mutableListOf(
                        Message.newBuilder().setFrom(1).setTo(1)
                                .setType(MessageType.MsgProp)
                                .addAllEntries(mutableListOf(ProtoBufUtils.buildEntry("somedata".toByteArray())))
                                .build(),

                        Message.newBuilder().setFrom(1).setTo(2).setType(MessageType.MsgHup).build(),

                        Message.newBuilder().setFrom(1).setTo(2)
                                .setType(MessageType.MsgProp)
                                .addAllEntries(mutableListOf(ProtoBufUtils.buildEntry("somedata".toByteArray())))
                                .build()),
                        4))

        tests.forEachIndexed { i, tt ->
            tt.network.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build()))

            tt.msgs.forEachIndexed { _, m ->
                tt.network.send(mutableListOf(m))
            }

            tt.network.peers.forEach { j, sm ->
                val raft = sm.raft!!
                assertEquals(tt.wcommited, raft.raftLog.committed)
                val ents = mutableListOf<RaftProtoBuf.Entry>()
                val nextEnts = nextEnts(raft, tt.network.storage[j]!!)
                nextEnts.forEach { e: RaftProtoBuf.Entry ->
                    if (e.data != null && !e.data.isEmpty) {
                        ents.add(e)
                    }
                }

                val props = mutableListOf<RaftProtoBuf.Message>()
                tt.msgs.forEach { m: RaftProtoBuf.Message ->
                    if (m.type == MessageType.MsgProp) {
                        props.add(m)
                    }
                }

                props.forEachIndexed { k, m ->
                    assertEquals(m.entriesList[0].data, ents[k].data, "index:$i,$k")
                }
            }
        }
    }

    // TestLearnerLogReplication tests that a learner can receive entries from the leader.
    @Test
    fun testLearnerLogReplication() {
        val n1 = newTestLearnerRaft(1, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())
        val n2 = newTestLearnerRaft(2, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())

        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2)))

        n1.becomeFollower(1, 0)
        n2.becomeFollower(1, 0)

        setRandomizedElectionTimeout(n1, n1.electionTimeout)
        for (i in 0 until n1.electionTimeout) {
            n1.tick()
        }

        nt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgBeat).build()))

        // n1 is leader and n2 is learner
        assertEquals(LEADER, n1.state)
        assertTrue(n2.isLearner)

        val nextCommitted = n1.raftLog.committed + 1
        nt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp).addAllEntries(mutableListOf(ProtoBufUtils.buildEntry("somedata".toByteArray()))).build()))
        assertEquals(nextCommitted, n1.raftLog.committed)

        assertEquals(n1.raftLog.committed, n2.raftLog.committed)

        val match = n1.getProgress(2)!!.match
        assertEquals(n2.raftLog.committed, match)
    }

    @Test
    fun testSingleNodeCommit() {
        val tt = Network.newNetwork(mutableListOf(null))
        tt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build()))
        tt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp).addEntries(ProtoBufUtils.buildEntry("somedata".toByteArray())).build()))
        tt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp).addEntries(buildEntry("somedata".toByteArray())).build()))

        val sm = tt.peers[1]
        val raft = sm!!.raft!!
        assertEquals(3, raft.raftLog.committed)
    }

    // TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
    // when leader changes, no new proposal comes in and ChangeTerm proposal is
    // filtered.
    @Test
    fun testCannotCommitWithoutNewTermEntry() {
        val tt = Network.newNetwork(mutableListOf(null, null, null, null, null))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // 0 cannot reach 2,3,4
        tt.cut(1, 3)
        tt.cut(1, 4)
        tt.cut(1, 5)

        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))

        val sm = tt.peers[1]!!
        var raft = sm.raft!!
        assertEquals(1, raft.raftLog.committed)

        // network recovery
        tt.recover()
        // avoid committing ChangeTerm proposal
        tt.ignore(MessageType.MsgApp)

        // elect 2 as the new leader with term 2
        tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))

        // no log entries from previous term should be committed
        raft = tt.peers[2]!!.raft!!
        assertEquals(1, raft.raftLog.committed)


        tt.recover()
        // send heartbeat; reset wait
        tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgBeat)))
        // append an entry at current term
        tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        // expect the committed to be advanced
        assertEquals(5, raft.raftLog.committed)
    }

    // TestCommitWithoutNewTermEntry tests the entries could be committed
    // when leader changes, no new proposal comes in.
    @Test
    fun testCommitWithoutNewTermEntry() {
        val tt = Network.newNetwork(mutableListOf(null, null, null, null, null))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // 0 cannot reach 2,3,4
        tt.cut(1, 3)
        tt.cut(1, 4)
        tt.cut(1, 5)

        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))

        val sm = tt.peers[1]!!
        val raft = sm.raft!!
        assertEquals(1, raft.raftLog.committed)

        // network recovery
        tt.recover()

        // elect 1 as the new leader with term 2
        // after append a ChangeTerm entry from the current term, all entries
        // should be committed
        tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))

        assertEquals(4, raft.raftLog.committed)
    }

    @Test
    fun testDuelingCandidates() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        nt.cut(1, 3)

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // 1 becomes leader since it receives votes from 1 and 2
        var raft = nt.peers[1]!!.raft!!
        assertEquals(LEADER, raft.state)

        // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
        raft = nt.peers[3]!!.raft!!
        assertEquals(CANDIDATE, raft.state)


        nt.recover()


        // candidate 3 now increases its term and tries to vote again
        // we expect it to disrupt the leader 1 since it has a higher term
        // 3 will be follower again since both 1 and 2 rejects its vote request since 3 does not have a long enough log
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        val wlog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1))))
        wlog.committed = 1
        wlog.unstable = Unstable(2)

        class Item(val sm: Raft, val state: StateType, val term: Long, val raftLog: RaftLog)

        val tests = mutableListOf(
                Item(a, FOLLOWER, 2, wlog),
                Item(b, FOLLOWER, 2, wlog),
                Item(c, FOLLOWER, 2, RaftLog(MemoryRaftStorage())))

        tests.forEachIndexed { i, tt ->
            assertEquals(tt.state, tt.sm.state)
            assertEquals(tt.term, tt.sm.term)
            val base = Utils.ltoa(tt.raftLog)
            val r = nt.peers[1 + i.toLong()]?.raft
            r?.run {
                val l = Utils.ltoa(this.raftLog)
                assertEquals(base, l, "index:$i")
            } ?: run {
                log.info { "#$i: empty log" }
            }
        }
    }

    @Test
    fun testDuelingPreCandidates() {
        val cfgA = newTestConfig(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val cfgB = newTestConfig(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val cfgC = newTestConfig(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        cfgA.preVote = true
        cfgB.preVote = true
        cfgC.preVote = true

        val a = Raft(cfgA)
        val b = Raft(cfgB)
        val c = Raft(cfgC)

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))

        nt.cut(1, 3)

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // 1 becomes leader since it receives votes from 1 and 2
        var raft = nt.peers[1]!!.raft!!
        assertEquals(LEADER, raft.state)

        // 3 campaigns then reverts to follower when its PreVote is rejected
        raft = nt.peers[3]!!.raft!!
        assertEquals(FOLLOWER, raft.state)

        nt.recover()

        // Candidate 3 now increases its term and tries to vote again.
        // With PreVote, it does not disrupt the leader.
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        val wlog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1))))
        wlog.committed = 1
        wlog.unstable = Unstable(2)

        class Item(val sm: Raft, val state: StateType, val term: Long, val raftLog: RaftLog)

        val tests = mutableListOf(
                Item(a, LEADER, 1, wlog),
                Item(b, FOLLOWER, 1, wlog),
                Item(c, FOLLOWER, 1, RaftLog(MemoryRaftStorage())))

        tests.forEachIndexed { i, tt ->
            assertEquals(tt.state, tt.sm.state)
            assertEquals(tt.term, tt.sm.term)
            val base = Utils.ltoa(tt.raftLog)

            val r = nt.peers[1 + i.toLong()]?.raft
            r?.run {
                val l = Utils.ltoa(this.raftLog)
                assertEquals(base, l, "index:$i")
            } ?: run {
                log.info { "#$i: empty log" }
            }
        }
    }

    @Test
    fun testCandidateConcede() {
        val tt = Network.newNetwork(mutableListOf(null, null, null))
        tt.isolate(1)

        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        tt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // heal the partition
        tt.recover()
        // send heartbeat; reset wait
        tt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgBeat)))

        val data = "force follower".toByteArray()
        // send a proposal to 3 to flush out a MsgApp to 1
        tt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgProp, mutableListOf(buildEntry(data)))))
        // send heartbeat; flush out commit
        tt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgBeat)))

        val a = tt.peers[1]!!.raft!!
        assertEquals(FOLLOWER, a.state)
        assertEquals(1, a.term)

        val wlog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1), buildEntry(2, 1, data))))
        wlog.committed = 2
        wlog.unstable = Unstable(3)

        val wantLog = Utils.ltoa(wlog)
        tt.peers.forEach { i, p ->
            val r = p.raft
            r?.run {
                val l = Utils.ltoa(r.raftLog)
                assertEquals(l, wantLog)
            } ?: run {
                "#$i: empty log"
            }
        }
    }

    @Test
    fun testSingleNodeCandidate() {
        val tt = Network.newNetwork(mutableListOf(null))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, tt.peers[1]!!.raft!!.state)
    }


    @Test
    fun testSingleNodePreCandidate() {
        val tt = Network.newNetworkWithConfig(mutableListOf(null), true)
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, tt.peers[1]!!.raft!!.state)
    }

    @Test
    fun testOldMessages() {
        val tt = Network.newNetwork(mutableListOf(null, null, null))
        // make 0 leader @ term 3
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
        // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
        tt.send(mutableListOf(buildMessage(2, 1, MessageType.MsgApp, 2, mutableListOf(buildEntry(3, 2)))))
        // commit a new entry
        tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))

        val iLog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 3), buildEntry(4, 3, "somedata".toByteArray()))))
        iLog.committed = 4
        iLog.unstable = Unstable(5)
        val base = Utils.ltoa(iLog)

        tt.peers.forEach { i, p ->
            val r = p.raft
            r?.run {
                val l = Utils.ltoa(r.raftLog)
                assertEquals(l, base)
            } ?: run {
                "#$i: empty log"
            }
        }
    }

    // TestOldMessagesReply - optimization - reply with new term.

    @Test
    fun testProposal() {
        class Item(val network: Network, val success: Boolean)

        val tests = mutableListOf(
                Item(Network.newNetwork(mutableListOf(null, null, null)), true),
                Item(Network.newNetwork(mutableListOf(null, null, StateMachine.NOP_STEPPER)), true),
                Item(Network.newNetwork(mutableListOf(null, StateMachine.NOP_STEPPER, StateMachine.NOP_STEPPER)), false),
                Item(Network.newNetwork(mutableListOf(null, StateMachine.NOP_STEPPER, StateMachine.NOP_STEPPER, null, null)), true))
        tests.forEachIndexed { j, tt ->
            val data = "somedata".toByteArray()
            // promote 0 the leader
            try {
                tt.network.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))
                tt.network.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry(data)))))
            } catch (e: Exception) {
                // only recover is we expect it to panic so
                // panics we don't expect go up.
                if (!tt.success) {
                    log.info { "#$j:error:$e" }
                }
            }

            var wantLog = RaftLog(MemoryRaftStorage())
            if (tt.success) {
                wantLog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1), buildEntry(2, 1, data))))
                wantLog.unstable = Unstable(3)
                wantLog.committed = 2
            }

            val base = Utils.ltoa(wantLog)
            tt.network.peers.forEach { i, p ->
                val r = p.raft
                r?.run {
                    val l = Utils.ltoa(r.raftLog)
                    assertEquals(l, base)
                } ?: run {
                    "#$i: empty log"
                }
            }

            val raft = tt.network.peers[1]!!.raft!!
            assertEquals(1, raft.term)
        }
    }

    @Test
    fun testProposalByProxy() {
        val data = "somedata".toByteArray()
        val tests = mutableListOf<Network>(
                Network.newNetwork(mutableListOf(null, null, null)),
                Network.newNetwork(mutableListOf(null, null, StateMachine.NOP_STEPPER)))
        tests.forEachIndexed { j, tt ->
            // promote 0 the leader
            tt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

            // promote 0 the leader
            tt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgProp, mutableListOf(buildEntry(data)))))
            val wantlog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.newBuilder().build(), buildEntry(1, 1), buildEntry(2, 1, data))))
            wantlog.unstable = Unstable(3)
            wantlog.committed = 2

            val base = Utils.ltoa(wantlog)
            tt.peers.forEach { i, p ->
                val r = p.raft
                r?.run {
                    val l = Utils.ltoa(r.raftLog)
                    assertEquals(l, base)
                } ?: run {
                    "#$i: empty log"
                }
            }

            val raft = tt.peers[1]!!.raft!!
            assertEquals(1, raft.term)
        }
    }

    @Test
    fun testCommit() {
        class Item(val matches: MutableList<Long>, val logs: MutableList<RaftProtoBuf.Entry>, val smTerm: Long, val w: Long)

        val tests = mutableListOf<Item>(
                // single
                Item(mutableListOf(1), mutableListOf(buildEntry(1, 1)), 1, 1),
                Item(mutableListOf(1), mutableListOf(buildEntry(1, 1)), 2, 0),
                Item(mutableListOf(2), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 2, 2),
                Item(mutableListOf(1), mutableListOf(buildEntry(1, 2)), 2, 1),

                // odd
                Item(mutableListOf(2, 1, 1), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 1, 1),
                Item(mutableListOf(2, 1, 1), mutableListOf(buildEntry(1, 1), buildEntry(2, 1)), 2, 0),
                Item(mutableListOf(2, 1, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 2, 2),
                Item(mutableListOf(2, 1, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 1)), 2, 0),

                // even
                Item(mutableListOf(2, 1, 1, 1), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 1, 1),
                Item(mutableListOf(2, 1, 1, 1), mutableListOf(buildEntry(1, 1), buildEntry(2, 1)), 2, 0),
                Item(mutableListOf(2, 1, 1, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 1, 1),
                Item(mutableListOf(2, 1, 1, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 1)), 2, 0),
                Item(mutableListOf(2, 1, 2, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 2)), 2, 2),
                Item(mutableListOf(2, 1, 2, 2), mutableListOf(buildEntry(1, 1), buildEntry(2, 1)), 2, 0))

        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.append(tt.logs)
            storage.setHardState(HardState.newBuilder().setTerm(tt.smTerm).build())

            val sm = StateMachine(newTestRaft(1, mutableListOf(1), 10, 2, storage))
            for (j in 0 until tt.matches.size) {
                sm.raft!!.setProgress(j.toLong() + 1, tt.matches[j], tt.matches[j] + 1, false)
            }

            sm.raft!!.maybeCommit()
            assertEquals(tt.w, sm.raft.raftLog.committed)
        }
    }

    @Test
    fun testPastElectionTimeout() {
        class Item(val elapse: Long, val wprobability: Double, val round: Boolean)

        val tests = mutableListOf<Item>(
                Item(5, 0.toDouble(), false),
                Item(10, 0.1, true),
                Item(13, 0.4, true),
                Item(15, 0.6, true),
                Item(18, 0.9, true),
                Item(20, 1.toDouble(), false))
        tests.forEachIndexed { i, tt ->
            val sm = StateMachine(newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage()))
            sm.raft!!.electionElapsed = tt.elapse
            var c = 0
            for (j in 0 until 10000) {
                sm.raft.resetRandomizedElectionTimeout()
                if (sm.raft.pastElectionTimeout()) {
                    c++
                }
            }
            var got = c.toDouble() / 10000.0
            if (tt.round) {
                got = Math.floor(got * 10F + 0.5) / 10.0
            }
            assertEquals(tt.wprobability, got)
        }
    }

    // ensure that the Step function ignores the message from old term and does not pass it to the
    // actual stepX function.
    @Test
    fun testStepIgnoreOldTermMsg() {
        val raft = spy(newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage()))
        val sm = StateMachine(raft)
        sm.raft!!.term = 2
        sm.raft.step(Message.newBuilder().setType(MessageType.MsgApp).setTerm(sm.raft.term - 1).build())
        //TODO:optimize mock to support find class and find method
        verify(raft, never()).stepLeader(Message.newBuilder().build())
        verify(raft, never()).stepFollower(Message.newBuilder().build())
        verify(raft, never()).stepFollower(Message.newBuilder().build())
    }

    // TestHandleMsgApp ensures:
    // 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
    // 2. If an existing entry conflicts with a new one (same index but different terms),
    //    delete the existing entry and all that follow it; append any new entries not already in the log.
    // 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
    @Test
    fun testHandleMsgApp() {
        class Item(val m: RaftProtoBuf.Message, val wIndex: Long, val wCommit: Long, val wReject: Boolean)

        val tests = mutableListOf<Item>(
                // Ensure 1
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(3).setIndex(2).setCommit(3).build(), 2, 0, true),  // previous log mismatch
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(3).setIndex(3).setCommit(3).build(), 2, 0, true),// previous log non-exist

                // Ensure 2
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(1).setIndex(1).setCommit(1).build(), 2, 1, false),
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(0).setIndex(0).setCommit(1).addAllEntries(mutableListOf(buildEntry(1, 2))).build(), 1, 1, false),
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(2).setIndex(2).setCommit(3).addAllEntries(mutableListOf(buildEntry(3, 2), buildEntry(4, 2))).build(), 4, 3, false),
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(2).setIndex(2).setCommit(4).addAllEntries(mutableListOf(buildEntry(3, 2))).build(), 3, 3, false),
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(1).setIndex(1).setCommit(4).addAllEntries(mutableListOf(buildEntry(2, 2))).build(), 2, 2, false),

                // Ensure 3
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(1).setLogTerm(1).setIndex(1).setCommit(3).build(), 2, 1, false), // match entry 1, commit up to last new entry 1
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(1).setLogTerm(1).setIndex(1).setCommit(3).addAllEntries(mutableListOf(buildEntry(2, 2))).build(), 2, 2, false),  // match entry 1, commit up to last new entry 2
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(2).setIndex(2).setCommit(3).build(), 2, 2, false),  // match entry 2, commit up to last new entry 2
                Item(Message.newBuilder().setType(MessageType.MsgApp).setTerm(2).setLogTerm(2).setIndex(2).setCommit(4).build(), 2, 2, false))   // commit up to log.last()

        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.append(mutableListOf(buildEntry(1, 1), buildEntry(2, 2)))
            val sm = StateMachine(newTestRaft(1, mutableListOf(1), 10, 1, storage))
            sm.raft!!.becomeFollower(1, 0)

            sm.raft.handleAppendEntries(tt.m)
            assertEquals(tt.wIndex, sm.raft.raftLog.lastIndex())
            assertEquals(tt.wCommit, sm.raft.raftLog.committed)
            val m = sm.readMessages()
            assertEquals(1, m.size)
            assertEquals(tt.wReject, m[0].reject)
        }
    }

    // TestHandleHeartbeat ensures that the follower commits to the commit in the message.
    @Test
    fun testHandleHeartbeat() {
        val commit = 2L

        class Item(val m: RaftProtoBuf.Message, val wCommit: Long)

        val tests = mutableListOf<Item>(
                Item(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgBeat).setTerm(2).setCommit(commit + 1).build(), commit + 1),
                Item(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgBeat).setTerm(2).setCommit(commit - 1).build(), commit)  // do not decrease commit
        )

        tests.forEachIndexed { i, tt ->
            val storage = MemoryRaftStorage()
            storage.append(mutableListOf(buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 3)))
            val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2), 5, 1, storage))
            val raft = sm.raft!!
            raft.becomeFollower(2, 2)
            raft.raftLog.commitTo(2)
            raft.handleHeartbeat(tt.m)
            assertEquals(tt.wCommit, raft.raftLog.committed)
            val m = sm.readMessages()
            assertEquals(1, m.size)
            assertEquals(MessageType.MsgHeartbeatResp, m[0].type)
        }
    }

    // TestHandleHeartbeatResp ensures that we re-send log entries when we get a heartbeat response.
    @Test
    fun testHandleHeartbeatResp() {
        val storage = MemoryRaftStorage()
        storage.append(mutableListOf(buildEntry(1, 1), buildEntry(2, 2), buildEntry(3, 3)))
        val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2), 5, 1, storage))
        val raft = sm.raft!!
        raft.becomeCandidate()
        raft.becomeLeader()
        raft.raftLog.commitTo(raft.raftLog.lastIndex())

        // A heartbeat response from a node that is behind; re-send MsgApp
        sm.step(buildMessage(2, MessageType.MsgHeartbeatResp))

        var msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        assertEquals(MessageType.MsgApp, msgs[0].type)

        // A second heartbeat response generates another MsgApp re-send
        sm.step(buildMessage(2, MessageType.MsgHeartbeatResp))
        msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        assertEquals(MessageType.MsgApp, msgs[0].type)

        // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
        sm.step(buildMessage(2, MessageType.MsgAppResp, msgs[0].index + msgs[0].entriesCount))
        // Consume the message sent in response to MsgAppResp
        sm.readMessages()

        sm.step(buildMessage(2, MessageType.MsgHeartbeatResp))
        msgs = sm.readMessages()
        assertEquals(0, msgs.size)
    }


    // TestRaftFreesReadOnlyMem ensures raft will free read request from
    // readOnly readIndexQueue and pendingReadIndex map.
    // related issue: https://go.etcd.io/etcd/issues/7571
    @Test
    fun testRaftFreesReadOnlyMem() {
        val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2), 5, 1, MemoryRaftStorage()))
        val raft = sm.raft!!
        raft.becomeCandidate()
        raft.becomeLeader()

        raft.raftLog.commitTo(raft.raftLog.lastIndex())

        val ctx = "ctx".toByteArray()
        // leader starts linearizable read request.
        // more info: raft dissertation 6.4, step 2.
        raft.step(buildMessage(2, MessageType.MsgReadIndex, mutableListOf(buildEntry(ctx))))
        val msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        assertEquals(MessageType.MsgHeartbeat, msgs[0].type)
        assertEquals(String(ctx), String(msgs[0].context.toByteArray()))
        assertEquals(1, raft.readonly.readIndexQueue.size)
        assertEquals(1, raft.readonly.pendingReadIndex.size)
        assertTrue(raft.readonly.pendingReadIndex.containsKey(String(ctx)))

        // heartbeat responses from majority of followers (1 in this case)
        // acknowledge the authority of the leader.
        // more info: raft dissertation 6.4, step 3.
        sm.step(buildMessage(2, MessageType.MsgHeartbeatResp, ctx))
        assertEquals(0, raft.readonly.readIndexQueue.size)
        assertEquals(0, raft.readonly.pendingReadIndex.size)
        assertTrue(!raft.readonly.pendingReadIndex.containsKey(String(ctx)))
    }

    // TestMsgAppRespWaitReset verifies the resume behavior of a leader
    // MsgAppResp.
    @Test
    fun testMsgAppRespWaitReset() {
        val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 5, 1, MemoryRaftStorage()))
        val raft = sm.raft!!
        raft.becomeCandidate()
        raft.becomeLeader()

        // The new leader has just emitted a new Term 4 entry; consume those messages
        // from the outgoing queue.
        sm.step(buildMessage(2, MessageType.MsgAppResp, 1))

        assertEquals(1, raft.raftLog.committed)
        // Also consume the MsgApp messages that update Commit on the followers.
        sm.readMessages()

        // A new command is now proposed on node 1.
        sm.step(buildMessage(1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance())))

        // The command is broadcast to all nodes not in the wait state.
        // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
        var msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        if (msgs[0].type != MessageType.MsgApp || msgs[0].to != 2L) {
            fail("expected MsgApp to node 2, got ${msgs[0].type} to ${msgs[0].to}")
        }

        if (msgs[0].entriesCount != 1 || msgs[0].entriesList[0].index != 2L) {
            fail("expected to send entry 2, but got ${msgs[0].entriesCount}")
        }

        sm.step(buildMessage(3, MessageType.MsgAppResp, 1))
        msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        if (msgs[0].type != MessageType.MsgApp || msgs[0].to != 3L) {
            fail("expected MsgApp to node 3, got ${msgs[0].type} to ${msgs[0].to}")
        }
        if (msgs[0].entriesCount != 1 || msgs[0].entriesList[0].index != 2L) {
            fail("expected to send entry 2, but got ${msgs[0].entriesCount}")
        }
    }


    @Test
    fun testRecvMsgVote() {
        testRecvMsgVote(MessageType.MsgVote)
    }

    @Test
    fun testRecvMsgPreVote() {
        testRecvMsgVote(MessageType.MsgPreVote)
    }

    fun testRecvMsgVote(msgType: RaftProtoBuf.MessageType) {
        class Item(val state: StateType, val index: Long, val logTerm: Long, val voteFor: Long, val wreject: Boolean)

        val tests = mutableListOf<Item>(
                Item(FOLLOWER, 0, 0, 0, true),
                Item(FOLLOWER, 0, 1, 0, true),
                Item(FOLLOWER, 0, 2, 0, true),
                Item(FOLLOWER, 0, 3, 0, false),

                Item(FOLLOWER, 1, 0, 0, true),
                Item(FOLLOWER, 1, 1, 0, true),
                Item(FOLLOWER, 1, 2, 0, true),
                Item(FOLLOWER, 1, 3, 0, false),

                Item(FOLLOWER, 2, 0, 0, true),
                Item(FOLLOWER, 2, 1, 0, true),
                Item(FOLLOWER, 2, 2, 0, false),
                Item(FOLLOWER, 2, 3, 0, false),

                Item(FOLLOWER, 3, 0, 0, true),
                Item(FOLLOWER, 3, 1, 0, true),
                Item(FOLLOWER, 3, 2, 0, false),
                Item(FOLLOWER, 3, 3, 0, false),

                Item(FOLLOWER, 3, 2, 2, false),
                Item(FOLLOWER, 3, 2, 1, true),

                Item(LEADER, 3, 3, 1, true),
                Item(PRE_CANDIDATE, 3, 3, 1, true),
                Item(CANDIDATE, 3, 3, 1, true))

        tests.forEachIndexed { i, tt ->
            val sm = StateMachine(newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage()))
            val raft = sm.raft!!
            raft.state = tt.state
            raft.vote = tt.voteFor
            val raftLog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.getDefaultInstance(), buildEntry(1, 2), buildEntry(2, 2))))
            raftLog.unstable = Unstable(3)
            raft.raftLog = raftLog

            // raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
            // test we're only testing MsgVote responses when the campaigning node
            // has a different raft log compared to the recipient node.
            // Additionally we're verifying behaviour when the recipient node has
            // already given out its vote for its current term. We're not testing
            // what the recipient node does when receiving a message with a
            // different term number, so we simply initialize both term numbers to
            // be the same.
            val term = Math.max(raft.raftLog.lastTerm(), tt.logTerm)
            raft.term = term
            sm.step(Message.newBuilder().setType(msgType).setTerm(term).setFrom(2).setIndex(tt.index).setLogTerm(tt.logTerm).build())

            val msgs = sm.readMessages()
            assertEquals(1, msgs.size)
            assertEquals(Utils.voteRespMsgType(msgType), msgs[0].type)
            assertEquals(tt.wreject, msgs[0].reject)
        }
    }

    @Test
    fun testStateTransition() {
        class Item(val from: StateType, val to: StateType, val wallow: Boolean, val wterm: Long, val wlead: Long)

        val tests = mutableListOf<Item>(
                Item(FOLLOWER, FOLLOWER, true, 1, 0),
                Item(FOLLOWER, PRE_CANDIDATE, true, 0, 0),
                Item(FOLLOWER, CANDIDATE, true, 1, 0),
                Item(FOLLOWER, LEADER, false, 0, 0),

                Item(PRE_CANDIDATE, FOLLOWER, true, 0, 0),
                Item(PRE_CANDIDATE, PRE_CANDIDATE, true, 0, 0),
                Item(PRE_CANDIDATE, CANDIDATE, true, 1, 0),
                Item(PRE_CANDIDATE, LEADER, true, 0, 1),

                Item(CANDIDATE, FOLLOWER, true, 0, 0),
                Item(CANDIDATE, PRE_CANDIDATE, true, 0, 0),
                Item(CANDIDATE, CANDIDATE, true, 1, 0),
                Item(CANDIDATE, LEADER, true, 0, 1),

                Item(LEADER, FOLLOWER, true, 1, 0),
                Item(LEADER, PRE_CANDIDATE, false, 0, 0),
                Item(LEADER, CANDIDATE, false, 1, 0),
                Item(LEADER, LEADER, true, 0, 1))

        tests.forEachIndexed { i, tt ->
            try {
                val sm = StateMachine(newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage()))
                val raft = sm.raft!!
                raft.state = tt.from
                if (tt.to == FOLLOWER) {
                    raft.becomeFollower(tt.wterm, tt.wlead)
                } else if (tt.to == PRE_CANDIDATE) {
                    raft.becomePreCandidate()
                } else if (tt.to == CANDIDATE) {
                    raft.becomeCandidate()
                } else if (tt.to == LEADER) {
                    raft.becomeLeader()
                }


                assertEquals(tt.wterm, raft.term)
                assertEquals(tt.wlead, raft.lead)
            } catch (e: Exception) {
                if (tt.wallow) {
                    log.error(String.format("%d: allow = %v, want %v", i, false, true))
                }
            }
        }
    }

    @Test
    fun testAllServerStepdown() {
        class Item(val state: StateType, val wstate: StateType, val wterm: Long, val windex: Long)

        val tests = mutableListOf<Item>(
                Item(FOLLOWER, FOLLOWER, 3, 0),
                Item(PRE_CANDIDATE, FOLLOWER, 3, 0),
                Item(CANDIDATE, FOLLOWER, 3, 0),
                Item(LEADER, FOLLOWER, 3, 1))

        val tmsgTypes = mutableListOf<RaftProtoBuf.MessageType>(MessageType.MsgVote, MessageType.MsgApp)
        val tterm = 3L

        tests.forEachIndexed { i, tt ->
            val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage()))
            val raft = sm.raft!!
            when {
                tt.state == FOLLOWER -> raft.becomeFollower(1, 0)
                tt.state == PRE_CANDIDATE -> raft.becomePreCandidate()
                tt.state == CANDIDATE -> raft.becomeCandidate()
                tt.state == LEADER -> {
                    raft.becomeCandidate()
                    raft.becomeLeader()
                }
            }

            tmsgTypes.forEachIndexed { j, msgType ->
                sm.step(Message.newBuilder().setFrom(2).setType(msgType).setTerm(tterm).setLogTerm(tterm).build())

                assertEquals(tt.wstate, raft.state)
                assertEquals(tt.wterm, raft.term)
                assertEquals(tt.windex, raft.raftLog.lastIndex())
                assertEquals(tt.windex, raft.raftLog.allEntries().size.toLong())
                var wlead = 2L
                if (msgType == MessageType.MsgVote) {
                    wlead = 0L
                }

                assertEquals(wlead, raft.lead)
            }
        }
    }

    @Test
    fun testCandidateResetTermMsgHeartbeat() {
        testCandidateResetTerm(MessageType.MsgHeartbeat)
    }

    @Test
    fun testCandidateResetTermMsgApp() {
        testCandidateResetTerm(MessageType.MsgApp)
    }

    // testCandidateResetTerm tests when a candidate receives a
    // MsgHeartbeat or MsgApp from leader, "Step" resets the term
    // with leader's and reverts back to follower.
    fun testCandidateResetTerm(mt: RaftProtoBuf.MessageType) {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(FOLLOWER, c.state)


        // isolate 3 and increase term in rest
        nt.isolate(3)

        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)

        // trigger campaign in isolated c
        c.resetRandomizedElectionTimeout()
        for (i in 0 until c.randomizedElectionTimeout) {
            c.tick()
        }

        assertEquals(CANDIDATE, c.state)

        nt.recover()

        // leader sends to isolated candidate
        // and expects candidate to revert to follower
        nt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(3).setTerm(a.term).setType(mt).build()))

        assertEquals(FOLLOWER, c.state)
        assertEquals(c.term, a.term)
    }

    @Test
    fun testLeaderStepdownWhenQuorumActive() {
        val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 5, 1, MemoryRaftStorage()))

        val raft = sm.raft!!
        raft.checkQuorum = true
        raft.becomeCandidate()
        raft.becomeLeader()

        for (i in 0 until (raft.electionTimeout + 1)) {
            sm.step(Message.newBuilder().setFrom(2).setType(MessageType.MsgHeartbeatResp).setTerm(raft.term).build())
            raft.tick()
        }

        assertEquals(LEADER, raft.state)
    }

    @Test
    fun testLeaderStepdownWhenQuorumLost() {
        val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 5, 1, MemoryRaftStorage()))

        val raft = sm.raft!!
        raft.checkQuorum = true
        raft.becomeCandidate()
        raft.becomeLeader()

        for (i in 0 until (raft.electionTimeout + 1)) {
            raft.tick()
        }

        assertEquals(FOLLOWER, raft.state)
    }

    @Test
    fun testLeaderSupersedingWithCheckQuorum() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        setRandomizedElectionTimeout(b, b.electionTimeout + 1)
        for (i in 0 until b.electionTimeout) {
            b.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, c.state)

        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
        assertEquals(CANDIDATE, c.state)

        // Letting b's electionElapsed reach to electionTimeout
        for (i in 0 until b.electionTimeout) {
            b.tick()
        }

        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        assertEquals(LEADER, c.state)
    }

    @Test
    fun testLeaderElectionWithCheckQuorum() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        setRandomizedElectionTimeout(a, a.electionTimeout + 1)
        setRandomizedElectionTimeout(b, b.electionTimeout + 2)

        // Immediately after creation, votes are cast regardless of the
        // election timeout.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, c.state)

        // need to reset randomizedElectionTimeout larger than electionTimeout again,
        // because the value might be reset to electionTimeout since the last state changes
        setRandomizedElectionTimeout(a, a.electionTimeout + 1)
        setRandomizedElectionTimeout(b, b.electionTimeout + 2)
        for (i in 0 until a.electionTimeout) {
            a.tick()
        }

        for (i in 0 until b.electionTimeout) {
            b.tick()
        }
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        assertEquals(FOLLOWER, a.state)
        assertEquals(LEADER, c.state)
    }

    // TestFreeStuckCandidateWithCheckQuorum ensures that a candidate with a higher term
    // can disrupt the leader even if the leader still "officially" holds the lease, The
    // leader is expected to step down and adopt the candidate's term
    @Test
    fun testFreeStuckCandidateWithCheckQuorum() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        setRandomizedElectionTimeout(b, b.electionTimeout + 1)
        for (i in 0 until b.electionTimeout) {
            b.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(1)
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        assertEquals(FOLLOWER, b.state)
        assertEquals(CANDIDATE, c.state)
        assertEquals(b.term + 1, c.term)

        // Vote again for safety
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        assertEquals(FOLLOWER, b.state)
        assertEquals(CANDIDATE, c.state)
        assertEquals(b.term + 2, c.term)

        nt.recover()
        nt.send(mutableListOf(buildMessage(1, 3, MessageType.MsgHeartbeat, a.term, mutableListOf())))

        // Disrupt the leader so that the stuck peer is freed
        assertEquals(FOLLOWER, a.state)
        assertEquals(a.term, c.term)

        // Vote again, should become leader this time
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        assertEquals(LEADER, c.state)
    }

    @Test
    fun testNonPromotableVoterWithCheckQuorum() {
        val a = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1), 10, 1, MemoryRaftStorage())

        a.checkQuorum = true
        b.checkQuorum = true

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b)))

        setRandomizedElectionTimeout(b, b.electionTimeout + 1)
        // Need to remove 2 again to make it a non-promotable node since newNetwork overwritten some internal states
        b.delProgress(2)

        assertFalse(b.promotable())

        for (i in 0 until b.electionTimeout) {
            b.tick()
        }
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(1, b.lead)
    }

    // TestDisruptiveFollower tests isolated follower,
    // with slow network incoming from leader, election times out
    // to become a candidate with an increased term. Then, the
    // candiate's response to late leader heartbeat forces the leader
    // to step down.
    @Test
    fun testDisruptiveFollower() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        a.becomeFollower(1, 0)
        b.becomeFollower(1, 0)
        c.becomeFollower(1, 0)

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateFollower
        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(FOLLOWER, c.state)

        // etcd server "advanceTicksForElection" on restart;
        // this is to expedite campaign trigger when given larger
        // election timeouts (e.g. multi-datacenter deploy)
        // Or leader messages are being delayed while ticks elapse
        setRandomizedElectionTimeout(c, c.electionTimeout + 2)
        for (i in 0 until (c.randomizedElectionTimeout - 1)) {
            c.tick()
        }

        // ideally, before last election tick elapses,
        // the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
        // from leader n1, and then resets its "electionElapsed"
        // however, last tick may elapse before receiving any
        // messages from leader, thus triggering campaign
        c.tick()

        // n1 is still leader yet
        // while its heartbeat to candidate n3 is being delayed

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateCandidate
        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(CANDIDATE, c.state)

        // check term
        // n1.Term == 2
        // n2.Term == 2
        // n3.Term == 3
        assertEquals(2, a.term)
        assertEquals(2, b.term)
        assertEquals(3, c.term)

        // while outgoing vote requests are still queued in n3,
        // leader heartbeat finally arrives at candidate n3
        // however, due to delayed network from leader, leader
        // heartbeat was sent with lower term than candidate's
        nt.send(mutableListOf(buildMessage(1, 3, MessageType.MsgHeartbeat, a.term, mutableListOf())))

        // then candidate n3 responds with "pb.MsgAppResp" of higher term
        // and leader steps down from a message with higher term
        // this is to disrupt the current leader, so that candidate
        // with higher term can be freed with following election

        // check state
        // n1.state == StateFollower
        // n2.state == StateFollower
        // n3.state == StateCandidate
        assertEquals(FOLLOWER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(CANDIDATE, c.state)

        // check term
        // n1.Term == 3
        // n2.Term == 2
        // n3.Term == 3
        assertEquals(3, a.term)
        assertEquals(2, b.term)
        assertEquals(3, c.term)
    }

    // TestDisruptiveFollowerPreVote tests isolated follower,
    // with slow network incoming from leader, election times out
    // to become a pre-candidate with less log than current leader.
    // Then pre-vote phase prevents this isolated node from forcing
    // current leader to step down, thus less disruptions.
    @Test
    fun testDisruptiveFollowerPreVote() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        a.becomeFollower(1, 0)
        b.becomeFollower(1, 0)
        c.becomeFollower(1, 0)

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateFollower
        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(FOLLOWER, c.state)

        nt.isolate(3)
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray())))))
        a.preVote = true
        b.preVote = true
        c.preVote = true
        nt.recover()
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StatePreCandidate
        assertEquals(LEADER, a.state)
        assertEquals(FOLLOWER, b.state)
        assertEquals(PRE_CANDIDATE, c.state)

        // check term
        // n1.Term == 2
        // n2.Term == 2
        // n3.Term == 2
        assertEquals(2, a.term)
        assertEquals(2, b.term)
        assertEquals(2, c.term)

        // delayed leader heartbeat does not force current leader to step down
        nt.send(mutableListOf(buildMessage(1, 3, MessageType.MsgHeartbeat)))
        assertEquals(LEADER, a.state)
    }

    @Test
    fun testReadOnlyOptionSafe() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        setRandomizedElectionTimeout(b, b.electionTimeout + 1)

        for (i in 0 until b.electionTimeout) {
            b.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)
        class Item(val sm: StateMachine, val proposals: Int, val wri: Long, val wctx: ByteArray)

        val tests = mutableListOf<Item>(
                Item(StateMachine(a), 10, 11, "ctx1".toByteArray()),
                Item(StateMachine(b), 10, 21, "ctx2".toByteArray()),
                Item(StateMachine(c), 10, 31, "ctx3".toByteArray()),
                Item(StateMachine(a), 10, 41, "ctx4".toByteArray()),
                Item(StateMachine(b), 10, 51, "ctx5".toByteArray()),
                Item(StateMachine(c), 10, 61, "ctx6".toByteArray()))

        tests.forEachIndexed { i, tt ->
            for (j in 0 until tt.proposals) {
                nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
            }

            nt.send(mutableListOf(buildMessage(tt.sm.raft!!.id, tt.sm.raft.id, MessageType.MsgReadIndex, mutableListOf(buildEntry(tt.wctx)))))
            val r = tt.sm.raft
            assertTrue(r.readStates.size != 0)
            val rs = r.readStates[0]
            assertEquals(tt.wri, rs.index)
            assertEquals(String(tt.wctx), String(rs.requestCtx))
            r.readStates = mutableListOf()
        }
    }

    @Test
    fun testReadOnlyOptionLease() {
        val a = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val b = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val c = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        a.readonly.option = ReadOnlyOption.READ_ONLY_LEASE_BASED
        b.readonly.option = ReadOnlyOption.READ_ONLY_LEASE_BASED
        c.readonly.option = ReadOnlyOption.READ_ONLY_LEASE_BASED

        a.checkQuorum = true
        b.checkQuorum = true
        c.checkQuorum = true

        val nt = Network.newNetwork(mutableListOf(StateMachine(a), StateMachine(b), StateMachine(c)))
        setRandomizedElectionTimeout(b, b.electionTimeout + 1)

        for (i in 0 until b.electionTimeout) {
            b.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        assertEquals(LEADER, a.state)

        class Item(val sm: StateMachine, val proposals: Int, val wri: Long, val wctx: ByteArray)

        val tests = mutableListOf<Item>(
                Item(StateMachine(a), 10, 11, "ctx1".toByteArray()),
                Item(StateMachine(b), 10, 21, "ctx2".toByteArray()),
                Item(StateMachine(c), 10, 31, "ctx3".toByteArray()),
                Item(StateMachine(a), 10, 41, "ctx4".toByteArray()),
                Item(StateMachine(b), 10, 51, "ctx5".toByteArray()),
                Item(StateMachine(c), 10, 61, "ctx6".toByteArray()))

        tests.forEachIndexed { i, tt ->
            for (j in 0 until tt.proposals) {
                nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
            }

            nt.send(mutableListOf(buildMessage(tt.sm.raft!!.id, tt.sm.raft.id, MessageType.MsgReadIndex, mutableListOf(buildEntry(tt.wctx)))))
            val r = tt.sm.raft
            val rs = r.readStates[0]
            assertEquals(tt.wri, rs.index)
            assertEquals(String(tt.wctx), String(rs.requestCtx))
            r.readStates = mutableListOf()
        }
    }

    // TestReadOnlyForNewLeader ensures that a leader only accepts MsgReadIndex message
    // when it commits at least one log entry at it term.
    @Test
    fun testReadOnlyForNewLeader() {
        class NodeConfig(val id: Long, val commited: Long, val applied: Long, val compactIndex: Long)

        val tests = mutableListOf<NodeConfig>(
                NodeConfig(1, 1, 1, 0),
                NodeConfig(2, 2, 2, 2),
                NodeConfig(3, 2, 2, 2))
        val peers = mutableListOf<StateMachine>()
        tests.forEachIndexed { _, c ->
            val storage = MemoryRaftStorage()
            storage.append(mutableListOf(buildEntry(1, 1), buildEntry(2, 1)))
            storage.setHardState(HardState.newBuilder().setTerm(1).setCommit(c.commited).build())
            if (c.compactIndex != 0L) {
                storage.compact(c.compactIndex)
            }

            val cfg = newTestConfig(c.id, mutableListOf(1, 2, 3), 10, 1, storage)
            cfg.applied = c.applied
            val raft = Raft(cfg)
            peers.add(StateMachine(raft))
        }

        val nt = Network.newNetwork(peers)
        // Drop MsgApp to forbid peer a to commit any log entry at its term after it becomes leader.
        nt.ignore(MessageType.MsgApp)
        // Force peer a to become leader.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val raft = nt.peers[1]!!.raft!!
        assertEquals(LEADER, raft.state)

        // Ensure peer a drops read only request.
        val windex = 4L
        val wctx = "ctx".toByteArray()
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgReadIndex, mutableListOf(buildEntry(wctx)))))
        assertEquals(0, raft.readStates.size)

        nt.recover()

        // Force peer a to commit a log entry at its term
        for (i in 0 until raft.heartbeatTimeout) {
            raft.tick()
        }
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        assertEquals(4, raft.raftLog.committed)
        val lastLogTerm = raft.raftLog.zeroTermOnErrCompacted(raft.raftLog.term(raft.raftLog.committed).data, null)
        assertEquals(raft.term, lastLogTerm)

        // Ensure peer a accepts read only request after it commits a entry at its term.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgReadIndex, mutableListOf(buildEntry(wctx)))))

        assertEquals(1, raft.readStates.size)
        val rs = raft.readStates[0]
        assertEquals(windex, rs.index)
        assertEquals(String(wctx), String(rs.requestCtx))
    }

    @Test
    fun testLeaderAppResp() {
        // initial progress: match = 0; next = 3
        class Item(val index: Long, val reject: Boolean, val wmatch: Long, val wnext: Long, val wmsgnum: Int, val windex: Long, val wcommited: Long)

        val tests = mutableListOf<Item>(
                Item(3, true, 0, 3, 0, 0, 0),  // stale resp; no replies
                Item(2, true, 0, 2, 1, 1, 0),  // denied resp; leader does not commit; decrease next and send probing msg
                Item(2, false, 2, 4, 2, 2, 2), // accept resp; leader commits; broadcast with commit index
                Item(0, false, 0, 3, 0, 0, 0) // ignore heartbeat replies
        )
        tests.forEachIndexed { i, tt ->
            // sm term is 1 after it becomes the leader.
            // thus the last log term must be 1 to be committed.
            val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage()))
            val raft = sm.raft!!
            raft.raftLog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.getDefaultInstance(), buildEntry(1, 0), buildEntry(2, 1))))
            raft.raftLog.unstable = Unstable(3)

            raft.becomeCandidate()
            raft.becomeLeader()
            sm.readMessages()
            sm.step(Message.newBuilder().setFrom(2).setType(MessageType.MsgAppResp).setIndex(tt.index).setTerm(raft.term).setReject(tt.reject).setRejectHint(tt.index).build())

            val p = raft.prs[2]!!
            assertEquals(tt.wmatch, p.match)
            assertEquals(tt.wnext, p.next)

            val msgs = sm.readMessages()
            assertEquals(tt.wmsgnum, msgs.size)
            msgs.forEachIndexed { j, msg ->
                assertEquals(tt.windex, msg.index)
                assertEquals(tt.wcommited, msg.commit)
            }
        }
    }


    // When the leader receives a heartbeat tick, it should
    // send a MsgApp with m.Index = 0, m.LogTerm=0 and empty entries.
    @Test
    fun testBcastBeat() {
        val offset = 1000L
        // make a state machine with log.offset = 1000
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata.newBuilder().setIndex(offset).setTerm(1).setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2, 3))))
                .build()
        val storage = MemoryRaftStorage()
        storage.applySnapshot(s)
        val sm = StateMachine(newTestRaft(1, emptyList(), 10, 1, storage))
        val raft = sm.raft!!
        raft.term = 1

        raft.becomeCandidate()
        raft.becomeLeader()
        for (i in 0 until 10) {
            raft.appendEntry(mutableListOf(buildEntry(i.toLong() + i)))
        }

        // slow follower
        raft.prs[2]!!.match = 5
        raft.prs[2]!!.next = 6
        // normal follower
        raft.prs[3]!!.match = raft.raftLog.lastIndex()
        raft.prs[3]!!.next = raft.raftLog.lastIndex() + 1

        sm.step(Message.newBuilder().setType(MessageType.MsgBeat).build())
        val msgs = sm.readMessages()
        assertEquals(2, msgs.size)
        val wantCommitMap = mutableMapOf(Pair(2, min(raft.raftLog.committed, raft.prs[2]!!.match)), Pair(3, min(raft.raftLog.committed, raft.prs[3]!!.match)))

        msgs.forEachIndexed { i, m ->
            assertEquals(MessageType.MsgHeartbeat, m.type)
            assertEquals(0, m.index)
            assertEquals(0, m.logTerm)
            val a = wantCommitMap[m.to.toInt()]
            if (a == null || a == 0L) {
                fail(String.format("#%d: unexpected to %d", i, m.to))
            } else {
                assertEquals(a, m.commit)
                wantCommitMap.remove(m.to.toInt())
            }
            assertEquals(0, m.entriesCount)
        }
    }

    // tests the output of the state machine when receiving MsgBeat
    @Test
    fun testRecvMsgBeat() {
        class Item(val state: StateType, val wMsg: Int)

        val tests = mutableListOf<Item>(
                Item(LEADER, 2),
                // candidate and follower should ignore MsgBeat
                Item(CANDIDATE, 0),
                Item(FOLLOWER, 0))

        tests.forEachIndexed { i, tt ->
            val sm = StateMachine(newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage()))
            val raft = sm.raft!!
            raft.raftLog = RaftLog(MemoryRaftStorage(mutableListOf(Entry.getDefaultInstance(), buildEntry(1, 0), buildEntry(2, 1))))
            raft.term = 1
            raft.state = tt.state
            sm.step(buildMessage(1, 1, MessageType.MsgBeat))

            val msgs = sm.readMessages()
            assertEquals(tt.wMsg, msgs.size)
            msgs.forEachIndexed { _, m ->
                assertEquals(MessageType.MsgHeartbeat, m.type)
            }
        }
    }

    @Test
    fun testLeaderIncreaseNext() {
        val previousEnts = mutableListOf(buildEntry(1, 1), buildEntry(2, 1), buildEntry(3, 1))

        class Item(val state: ProgressStateType, val next: Long, val wnext: Long)

        val tests = mutableListOf(
                // state replicate, optimistically increase next
                // previous entries + noop entry + propose + 1
                Item(ProgressStateReplicate, 2, previousEnts.size + 3.toLong()),
                // state probe, not optimistically increase next
                Item(ProgressStateProbe, 2, 2L))

        tests.forEachIndexed { i, tt ->
            val raft = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
            val sm = StateMachine(raft)
            raft.raftLog.append(previousEnts)
            raft.becomeCandidate()
            raft.becomeLeader()
            raft.prs[2]!!.state = tt.state
            raft.prs[2]!!.next = tt.next
            sm.step(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(buildEntry("somedata".toByteArray()))))
            val p = raft.prs[2]!!
            assertEquals(tt.wnext, p.next)
        }
    }

    @Test
    fun testSendAppendForProgressProbe() {
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.becomeCandidate()
        r.becomeLeader()
        readMessages(r)
        r.prs[2]!!.becomeProbe()

        // each round is a heartbeat
        for (i in 0 until 3) {
            if (i == 0) {
                // we expect that raft will only send out one msgAPP on the first
                // loop. After that, the follower is paused until a heartbeat response is
                // received.
                r.appendEntry(mutableListOf(buildEntry("somedata".toByteArray())))
                r.sendAppend(2)
                val msg = readMessages(r)
                assertEquals(1, msg.size)
                assertEquals(0, msg[0].index)
            }

            assertTrue(r.prs[2]!!.paused)
            for (j in 0 until 10) {
                r.appendEntry(mutableListOf(buildEntry("somedata".toByteArray())))
                r.sendAppend(2)
                assertEquals(0, readMessages(r).size)
            }

            // do a heartbeat
            for (j in 0 until r.heartbeatTimeout) {
                r.step(buildMessage(1, 1, MessageType.MsgBeat))
            }

            assertTrue(r.prs[2]!!.paused)

            // consume the heartbeat
            val msg = readMessages(r)
            assertEquals(1, msg.size)
            assertEquals(MessageType.MsgHeartbeat, msg[0].type)
        }

        // a heartbeat response will allow another message to be sent
        r.step(buildMessage(2, 1, MessageType.MsgHeartbeatResp))
        val msg = readMessages(r)
        assertEquals(1, msg.size)
        assertEquals(0, msg[0].index)
        assertTrue(r.prs[2]!!.paused)
    }


    @Test
    fun testSendAppendForProgressReplicate() {
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.becomeCandidate()
        r.becomeLeader()
        readMessages(r)
        r.prs[2]!!.becomeReplicate()

        for (i in 0 until 10) {
            r.appendEntry(mutableListOf(buildEntry("somedata".toByteArray())))
            r.sendAppend(2)
            val msgs = readMessages(r)
            assertEquals(1, msgs.size)
        }
    }

    @Test
    fun testSendAppendForProgressSnapshot() {
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.becomeCandidate()
        r.becomeLeader()
        readMessages(r)
        r.prs[2]!!.becomeSnapshot(10)

        for (i in 0 until 10) {
            r.appendEntry(mutableListOf(buildEntry("somedata".toByteArray())))
            r.sendAppend(2)
            val msgs = readMessages(r)
            assertEquals(0, msgs.size)
        }
    }

    @Test
    fun testRecvMsgUnreachable() {
        val previousEnts = mutableListOf(buildEntry(1, 1), buildEntry(2, 1), buildEntry(3, 1))
        val s = MemoryRaftStorage()
        s.append(previousEnts)
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, s)
        r.becomeCandidate()
        r.becomeLeader()
        readMessages(r)
        // set node 2 to state replicate
        r.prs[2]!!.match = 2
        r.prs[2]!!.becomeReplicate()
        r.prs[2]!!.optimisticUpdate(5)

        r.step(buildMessage(2, 1, MessageType.MsgUnreachable))

        assertEquals(ProgressStateProbe, r.prs[2]!!.state)
        assertEquals(r.prs[2]!!.match + 1, r.prs[2]!!.next)
    }

    @Test
    fun testRestore() {
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2, 3))))
                .build()
        val storage = MemoryRaftStorage()
        val raft = newTestRaft(1, mutableListOf(1, 2), 10, 1, storage)
        assertTrue(raft.restore(s))

        assertEquals(s.metadata.index, raft.raftLog.lastIndex())
        assertEquals(s.metadata.term, mustTerm(raft.raftLog.term(s.metadata.index)))
        val sg = raft.nodes()
        assertEquals(s.metadata.confState.nodesList, sg)
        assertFalse(raft.restore(s))
    }


    // TestRestoreWithLearner restores a snapshot which contains learners.
    @Test
    fun testRestoreWithLearner() {
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2)).addAllLearners(mutableListOf(3))))
                .build()
        val storage = MemoryRaftStorage()
        val raft = newTestLearnerRaft(3, mutableListOf(1, 2), mutableListOf(3), 8, 2, storage)
        val sm = StateMachine(raft)
        assertTrue(raft.restore(s))

        assertEquals(s.metadata.index, raft.raftLog.lastIndex())
        assertEquals(s.metadata.term, mustTerm(raft.raftLog.term(s.metadata.index)))
        val sg = raft.nodes()
        assertEquals(s.metadata.confState.nodesCount, sg.size)
        val lns = raft.learnerNodes()
        assertEquals(s.metadata.confState.learnersCount, lns.size)
        s.metadata.confState.nodesList.forEach {
            if (raft.prs[it]!!.isLearner) {
                fail(String.format("sm.Node %x isLearner = %s, want %t", it, raft.prs[it], false))
            }
        }

        s.metadata.confState.learnersList.forEach {
            if (!raft.learnerPrs[it]!!.isLearner) {
                fail(String.format("sm.Node %x isLearner = %s, want %t", it, raft.prs[it], true))
            }
        }

        assertFalse(raft.restore(s))
    }

    // TestRestoreInvalidLearner verfies that a normal peer can't become learner again
    // when restores snapshot.
    @Test
    fun testRestoreInvalidLearner() {
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2)).addAllLearners(mutableListOf(3))))
                .build()
        val storage = MemoryRaftStorage()
        val raft = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, storage)
        assertFalse(raft.isLearner)
        assertFalse(raft.restore(s))
    }

    @Test
    fun testRestoreLearnerPromotion() {
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2, 3))))
                .build()
        val storage = MemoryRaftStorage()
        val raft = newTestLearnerRaft(3, mutableListOf(1, 2), mutableListOf(3), 10, 1, storage)

        assertTrue(raft.isLearner)
        assertTrue(raft.restore(s))
        assertFalse(raft.isLearner)
    }

    // TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader
    @Test
    fun testLearnerReceiveSnapshot() {
        // restore the state machine from a snapshot so it has a compacted log and a snapshot
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1)).addAllLearners(mutableListOf(2))))
                .build()

        val n1 = newTestLearnerRaft(1, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())
        val n2 = newTestLearnerRaft(2, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())

        n1.restore(s)

        // Force set n1 appplied index.
        n1.raftLog.appliedTo(n1.raftLog.committed)

        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2)))

        setRandomizedElectionTimeout(n1, n1.electionTimeout)
        for (i in 0 until n1.electionTimeout) {
            n1.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgBeat)))
        assertEquals(n1.raftLog.committed, n2.raftLog.committed)
    }

    @Test
    fun testRestoreIgnoreSnapshot() {
        val previousEnts = mutableListOf(buildEntry(1, 1), buildEntry(2, 1), buildEntry(3, 1))
        val commit = 1L
        val storage = MemoryRaftStorage()
        val raft = newTestRaft(1, mutableListOf(1, 2), 10, 1, storage)
        raft.raftLog.append(previousEnts)
        raft.raftLog.commitTo(commit)

        var s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(commit)
                        .setTerm(1)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2))))
                .build()

        // ignore snapshot
        assertFalse(raft.restore(s))
        assertEquals(commit, raft.raftLog.committed)

        // ignore snapshot and fast forward commit
        s = s.toBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(commit + 1)
                        .setTerm(1)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2))))
                .build()
        assertFalse(raft.restore(s))
        assertEquals(commit + 1, raft.raftLog.committed)
    }

    @Test
    fun testProvideSnap() {
        // restore the state machine from a snapshot so it has a compacted log and a snapshot
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2))))
                .build()

        val storage = MemoryRaftStorage()
        val raft = newTestRaft(1, mutableListOf(1), 10, 1, storage)
        val sm = StateMachine(raft)
        raft.restore(s)
        raft.becomeCandidate()
        raft.becomeLeader()

        // force set the next of node 2, so that node 2 needs a snapshot
        raft.prs[2]!!.next = raft.raftLog.firstIndex()
        sm.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgAppResp).setIndex(raft.prs[2]!!.next - 1).setReject(true).build())
        val msgs = sm.readMessages()
        assertEquals(1, msgs.size)
        assertEquals(MessageType.MsgSnap, msgs[0].type)
    }

    @Test
    fun testIgnoreProvidingSnap() {
        // restore the state machine from a snapshot so it has a compacted log and a snapshot
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2))))
                .build()
        val storage = MemoryRaftStorage()
        val raft = newTestRaft(1, mutableListOf(1), 10, 1, storage)
        raft.restore(s)

        raft.becomeCandidate()
        raft.becomeLeader()

        // force set the next of node 2, so that node 2 needs a snapshot
        // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
        raft.prs[2]!!.next = raft.raftLog.firstIndex() - 1
        raft.prs[2]!!.isRecentActive = false

        raft.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp).addAllEntries(mutableListOf(Entry.newBuilder().setData(ByteString.copyFrom("somedata".toByteArray())).build())).build())
        val msgs = readMessages(raft)
        assertEquals(0, msgs.size)
    }

    @Test
    fun testRestoreFromSnapMsg() {
        val s = Snapshot.newBuilder()
                .setMetadata(SnapshotMetadata
                        .newBuilder()
                        .setIndex(11)
                        .setTerm(11)
                        .setConfState(ConfState.newBuilder().addAllNodes(mutableListOf(1, 2))))

        val m = Message.newBuilder().setFrom(1).setTerm(2).setType(MessageType.MsgSnap).setSnapshot(s).build()

        val raft = newTestRaft(2, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        raft.step(m)
        assertEquals(1L, raft.lead)
    }

    @Test
    fun testSlowNodeRestore() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))


        nt.isolate(3)
        for (j in 0..100) {
            nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        }

        val lead = nt.peers[1]!!.raft!!
        nextEnts(lead, nt.storage[1]!!)
        nt.storage[1]!!.createSnapshot(lead.raftLog.applied, ConfState.newBuilder().addAllNodes(lead.nodes()).build(), ByteArray(0))
        nt.storage[1]!!.compact(lead.raftLog.applied)

        nt.recover()
        // send heartbeats so that the leader can learn everyone is active.
        // node 3 will only be considered as active when node 1 receives a reply from it.
        while (true) {
            nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgBeat)))
            if (lead.prs[3]!!.isRecentActive) {
                break
            }
        }

        // trigger a snapshot
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))

        val follower = nt.peers[3]!!.raft!!

        // trigger a commit
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        assertEquals(lead.raftLog.committed, follower.raftLog.committed)
    }

    // TestStepConfig tests that when raft step msgProp in EntryConfChange type,
    // it appends the entry to log and sets pendingConf to be true.
    @Test
    fun testStepConfig() {
        // a raft that cannot make progress
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.becomeCandidate()
        r.becomeLeader()
        val index = r.raftLog.lastIndex()
        r.step(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.newBuilder().setType(EntryType.EntryConfChange).build())))
        assertEquals(index + 1, r.raftLog.lastIndex())
        assertEquals(index + 1, r.pendingConfIndex)
    }


    // TestStepIgnoreConfig tests that if raft step the second msgProp in
    // EntryConfChange type when the first one is uncommitted, the node will set
    // the proposal to noop and keep its original state.
    @Test
    fun testStepIgnoreConfig() {
        // a raft that cannot make progress
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.becomeCandidate()
        r.becomeLeader()
        r.step(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.newBuilder().setType(EntryType.EntryConfChange).build())))
        val index = r.raftLog.lastIndex()
        val pendingConfIndex = r.pendingConfIndex
        r.step(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.newBuilder().setType(EntryType.EntryConfChange).build())))
        val wents = mutableListOf(Entry.newBuilder().setType(EntryType.EntryNormal).setTerm(1).setIndex(3).build())
        val result = r.raftLog.entries(index + 1, Long.MAX_VALUE)
        assertTrue(result.isSuccess, result.message)
        assertEquals(wents, result.data)
        assertEquals(pendingConfIndex, r.pendingConfIndex)
    }

    // TestNewLeaderPendingConfig tests that new leader sets its pendingConfigIndex
    // based on uncommitted entries.
    @Test
    fun testNewLeaderPendingConfig() {
        class Item(val addEntry: Boolean, val wpendingIndex: Long)

        val tests = mutableListOf(
                Item(false, 0L),
                Item(true, 1L))

        tests.forEachIndexed { i, tt ->
            val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
            if (tt.addEntry) {
                r.appendEntry(mutableListOf(Entry.newBuilder().setType(EntryType.EntryNormal).build()))
            }
            r.becomeCandidate()
            r.becomeLeader()
            assertEquals(tt.wpendingIndex, r.pendingConfIndex)
        }
    }

    @Test
    fun testAddNode() {
        // TestAddNode tests that addNode could update nodes correctly.
        val r = newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage())
        r.addNode(2)
        val nodes = r.nodes()
        val wnodes = mutableListOf(1L, 2L)
        assertEquals(wnodes, nodes)
    }

    // TestAddLearner tests that addLearner could update nodes correctly.
    @Test
    fun testAddLearner() {
        val r = newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage())
        r.addLearner(2)
        val nodes = r.learnerNodes()
        val wnodes = mutableListOf(2L)
        assertEquals(wnodes, nodes)
        assertEquals(true, r.learnerPrs[2]!!.isLearner)
    }

    // TestAddNodeCheckQuorum tests that addNode does not trigger a leader election
    // immediately when checkQuorum is set.
    @Test
    fun testAddNodeCheckQuorum() {
        val r = newTestRaft(1, mutableListOf(1), 10, 1, MemoryRaftStorage())
        r.checkQuorum = true

        r.becomeCandidate()
        r.becomeLeader()


        for (i in 0 until r.electionTimeout - 1) {
            r.tick()
        }

        r.addNode(2L)


        // This tick will reach electionTimeout, which triggers a quorum check.
        r.tick()

        // Node 1 should still be the leader after a single tick.
        assertEquals(LEADER, r.state)

        // After another electionTimeout ticks without hearing from node 2,
        // node 1 should step down.
        for (i in 0 until r.electionTimeout) {
            r.tick()
        }

        assertEquals(FOLLOWER, r.state)
    }

    // TestRemoveNode tests that removeNode could update nodes and
    // and removed list correctly.
    @Test
    fun testRemoveNode() {
        val r = newTestRaft(1, mutableListOf(1, 2), 10, 1, MemoryRaftStorage())
        r.removeNode(2)
        var w = mutableListOf(1L)
        assertEquals(w, r.nodes())

        // remove all nodes from cluster
        r.removeNode(1)
        w = mutableListOf()
        assertEquals(w, r.nodes())
    }

    // TestRemoveLearner tests that removeNode could update nodes and
    // and removed list correctly.
    @Test
    fun testRemoveLearner() {
        val r = newTestLearnerRaft(1, mutableListOf(1), mutableListOf(2), 10, 1, MemoryRaftStorage())
        r.removeNode(2)
        var w = mutableListOf(1L)
        assertEquals(w, r.nodes())

        w = mutableListOf()
        assertEquals(w, r.learnerNodes())

        // remove all nodes from cluster
        r.removeNode(1)
        assertEquals(w, r.nodes())
    }

    @Test
    fun testPromotable() {
        val id = 1L

        class Item(val peers: MutableList<Long>, val wp: Boolean)

        val tests = mutableListOf(
                Item(mutableListOf(1), true),
                Item(mutableListOf(1, 2, 3), true),
                Item(mutableListOf(), false),
                Item(mutableListOf(2, 3), false))

        tests.forEachIndexed { i, tt ->
            val r = newTestRaft(id, tt.peers, 5, 1, MemoryRaftStorage())
            assertEquals(tt.wp, r.promotable())
        }
    }

    @Test
    fun testRaftNodes() {
        class Item(val ids: MutableList<Long>, val wids: MutableList<Long>)

        val tests = mutableListOf(
                Item(mutableListOf(1, 2, 3), mutableListOf(1, 2, 3)),
                Item(mutableListOf(3, 2, 1), mutableListOf(1, 2, 3)))

        tests.forEachIndexed { i, tt ->
            val r = newTestRaft(1, tt.ids, 10, 1, MemoryRaftStorage())
            assertEquals(tt.wids, r.nodes())
        }
    }

    @Test
    fun testCampaignWhileLeader() {
        testCampaignWhileLeader(false)
    }

    @Test
    fun testPreCampaignWhileLeader() {
        testCampaignWhileLeader(true)
    }

    fun testCampaignWhileLeader(preVote: Boolean) {
        val cfg = newTestConfig(1, mutableListOf(1), 5, 1, MemoryRaftStorage())
        cfg.preVote = true
        val r = Raft(cfg)
        assertEquals(FOLLOWER, r.state)

        // We don't call campaign() directly because it comes after the check
        // for our current state.
        r.step(buildMessage(1, 1, MessageType.MsgHup))
        assertEquals(LEADER, r.state)
        val term = r.term
        r.step(buildMessage(1, 1, MessageType.MsgHup))
        assertEquals(LEADER, r.state)
        assertEquals(term, r.term)
    }

    // TestCommitAfterRemoveNode verifies that pending commands can become
    // committed when a config change reduces the quorum requirements.
    @Test
    fun testCommitAfterRemoveNode() {
        // Create a cluster with two nodes.
        val s = MemoryRaftStorage()
        val r = newTestRaft(1, mutableListOf(1, 2), 5, 1, s)
        r.becomeCandidate()
        r.becomeLeader()

        // Begin to remove the second node.
        val cc = ConfChange.newBuilder()
                .setType(ConfChangeType.ConfChangeRemoveNode)
                .setNodeID(2)
                .build()

        // Stabilize the log and make sure nothing is committed yet.
        r.step(Message.newBuilder()
                .setType(MessageType.MsgProp)
                .addAllEntries(mutableListOf(Entry.newBuilder().setType(EntryType.EntryConfChange).setData(cc.toByteString()).build()))
                .build())

        assertEquals(0, nextEnts(r, s).size)
        val ccIndex = r.raftLog.lastIndex()

        // While the config change is pending, make another proposal.
        r.step(Message.newBuilder()
                .setType(MessageType.MsgProp)
                .addAllEntries(mutableListOf(Entry.newBuilder().setType(EntryType.EntryNormal).setData(ByteString.copyFrom("hello".toByteArray())).build()))
                .build())

        // Node 2 acknowledges the config change, committing it.
        r.step(Message.newBuilder().setType(MessageType.MsgAppResp).setFrom(2).setIndex(ccIndex).build())
        var ents = nextEnts(r, s)
        assertEquals(2, ents.size)
        if (ents[0].type != EntryType.EntryNormal || ents[0].data != ByteString.EMPTY) {
            fail("expected ents[0] to be empty, but got ${ents[0]}")
        }

        assertEquals(EntryType.EntryConfChange, ents[1].type)

        // Apply the config change. This reduces quorum requirements so the
        // pending command can now commit.
        r.removeNode(2)
        ents = nextEnts(r, s)
        if (ents.size != 1 || ents[0].type != EntryType.EntryNormal || ents[0].data.toStringUtf8() != "hello") {
            fail("expected one committed EntryNormal, got ${ents}")
        }
    }

    // TestLeaderTransferToUpToDateNode verifies transferring should succeed
    // if the transferee has the most up-to-date log entries when transfer starts.
    @Test
    fun testLeaderTransferToUpToDateNode() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val lead = nt.peers[1]!!.raft!!
        assertEquals(1, lead.lead)

        // Transfer leadership to 2.
        nt.send(mutableListOf(buildMessage(2, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, FOLLOWER, 2)

        // After some log replication, transfer leadership back to 1.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))

        nt.send(mutableListOf(buildMessage(1, 2, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, LEADER, 1)
    }

    // TestLeaderTransferToUpToDateNodeFromFollower verifies transferring should succeed
    // if the transferee has the most up-to-date log entries when transfer starts.
    // Not like TestLeaderTransferToUpToDateNode, where the leader transfer message
    // is sent to the leader, in this test case every leader transfer message is sent
    // to the follower.
    @Test
    fun testLeaderTransferToUpToDateNodeFromFollower() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val lead = nt.peers[1]!!.raft!!
        assertEquals(1, lead.lead)
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgTransferLeader)))

        checkLeaderTransferState(lead, FOLLOWER, 2)

        // After some log replication, transfer leadership back to 1.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgTransferLeader)))

        checkLeaderTransferState(lead, LEADER, 1)
    }

    // TestLeaderTransferWithCheckQuorum ensures transferring leader still works
    // even the current leader is still under its leader lease
    @Test
    fun testLeaderTransferWithCheckQuorum() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        for (i in 1 until 4) {
            val r = nt.peers[i.toLong()]!!.raft!!
            r.checkQuorum = true
            setRandomizedElectionTimeout(r, r.electionTimeout + i)
        }

        // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
        val f = nt.peers[2]!!.raft!!
        for (i in 0 until f.electionTimeout) {
            f.tick()
        }

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val lead = nt.peers[1]!!.raft!!
        assertEquals(1, lead.lead)

        // Transfer leadership to 2.
        nt.send(mutableListOf(buildMessage(2, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, FOLLOWER, 2)

        // After some log replication, transfer leadership back to 1.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        nt.send(mutableListOf(buildMessage(1, 2, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, LEADER, 1)
    }

    @Test
    fun testLeaderTransferToSlowFollower() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))

        nt.recover()
        val lead = nt.peers[1]!!.raft!!
        assertEquals(1, lead.prs[3]!!.match)

        // Transfer leadership to 3 when node 3 is lack of log.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, FOLLOWER, 3)
    }

    @Test
    fun testLeaderTransferAfterSnapshot() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        val lead = nt.peers[1]!!.raft!!
        nextEnts(lead, nt.storage[1]!!)
        nt.storage[1]!!.createSnapshot(lead.raftLog.applied, ConfState.newBuilder().addAllNodes(lead.nodes()).build(), ByteArray(0))
        nt.storage[1]!!.compact(lead.raftLog.applied)

        nt.recover()
        assertEquals(1, lead.prs[3]!!.match)

        // Transfer leadership to 3 when node 3 is lack of snapshot.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        // Send pb.MsgHeartbeatResp to leader to trigger a snapshot for node 3.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgHeartbeatResp)))

        checkLeaderTransferState(lead, FOLLOWER, 3)
    }

    @Test
    fun testLeaderTransferToSelf() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val lead = nt.peers[1]!!.raft!!

        // Transfer leadership to self, there will be noop.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, LEADER, 1)
    }

    @Test
    fun testLeaderTransferToNonExistingNode() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        val lead = nt.peers[1]!!.raft!!
        // Transfer leadership to non-existing node, there will be noop.
        nt.send(mutableListOf(buildMessage(4, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, LEADER, 1)
    }

    @Test
    fun testLeaderTransferTimeout() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!

        // Transfer leadership to isolated node, wait for timeout.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)
        for (i in 0 until lead.electionTimeout) {
            lead.tick()
        }

        for (i in 0 until (lead.electionTimeout - lead.heartbeatTimeout)) {
            lead.tick()
        }

        checkLeaderTransferState(lead, LEADER, 1)
    }

    @Test
    fun testLeaderTransferIgnoreProposal() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!

        // Transfer leadership to isolated node, wait for timeout.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        val code = lead.step(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance())))
        assertEquals(ErrorCode.ErrProposalDropped, code)
        assertEquals(1, lead.prs[1]!!.match)
    }

    @Test
    fun testLeaderTransferReceiveHigherTermVote() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!

        // Transfer leadership to isolated node to let transfer pending.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        nt.send(mutableListOf(Message.newBuilder().setFrom(2).setTo(2).setType(MessageType.MsgHup).setIndex(1).setTerm(2).build()))
        checkLeaderTransferState(lead, FOLLOWER, 2)
    }

    @Test
    fun testLeaderTransferRemoveNode() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.ignore(MessageType.MsgTimeoutNow)

        val lead = nt.peers[1]!!.raft!!

        // The leadTransferee is removed when leadship transferring.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        lead.removeNode(3)

        checkLeaderTransferState(lead, LEADER, 1)
    }

    // TestLeaderTransferBack verifies leadership can transfer back to self when last transfer is pending.
    @Test
    fun testLeaderTransferBack() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!

        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        // Transfer leadership back to self.
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, LEADER, 1)
    }


    // TestLeaderTransferSecondTransferToAnotherNode verifies leader can transfer to another node
    // when last transfer is pending.
    @Test
    fun testLeaderTransferSecondTransferToAnotherNode() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!

        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        // Transfer leadership to another node.
        nt.send(mutableListOf(buildMessage(2, 1, MessageType.MsgTransferLeader)))
        checkLeaderTransferState(lead, FOLLOWER, 2)
    }

    // TestLeaderTransferSecondTransferToSameNode verifies second transfer leader request
    // to the same node should not extend the timeout while the first one is pending.
    @Test
    fun testLeaderTransferSecondTransferToSameNode() {
        val nt = Network.newNetwork(mutableListOf(null, null, null))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        nt.isolate(3)

        val lead = nt.peers[1]!!.raft!!
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))
        assertEquals(3, lead.leadTransferee)

        for (i in 0 until lead.heartbeatTimeout) {
            lead.tick()
        }

        // Second transfer leadership request to the same node.
        nt.send(mutableListOf(buildMessage(3, 1, MessageType.MsgTransferLeader)))

        for (i in 0 until (lead.electionTimeout - lead.heartbeatTimeout)) {
            lead.tick()
        }

        checkLeaderTransferState(lead, LEADER, 1)
    }

    private fun checkLeaderTransferState(r: Raft, state: StateType, lead: Long) {
        if (r.state != state || r.lead != lead) {
            fail("after transferring, node has state ${r.state} lead ${r.lead}, want state $state lead $lead")
        }

        if (r.leadTransferee != 0L) {
            fail("after transferring, node has leadTransferee ${r.leadTransferee}, want leadTransferee 0L")
        }
    }

    // TestTransferNonMember verifies that when a MsgTimeoutNow arrives at
    // a node that has been removed from the group, nothing happens.
    // (previously, if the node also got votes, it would panic as it
    // transitioned to StateLeader)
    @Test
    fun testTransferNonMember() {
        val r = newTestRaft(1, mutableListOf(2, 3, 4), 5, 1, MemoryRaftStorage())
        r.step(buildMessage(2, 1, MessageType.MsgTimeoutNow))

        r.step(buildMessage(2, 1, MessageType.MsgVoteResp))
        r.step(buildMessage(3, 1, MessageType.MsgVoteResp))
        assertEquals(FOLLOWER, r.state)
    }

    /**
     *    TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
     *     that has been partitioned away (and fallen behind) rejoins the cluster at
     *  about the same time the leader node gets partitioned away.
     *  Previously the cluster would come to a standstill when run with PreVote
     *  enabled.
     */
    @Test
    fun testNodeWithSmallerTermCanCompleteElection() {
        val n1 = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n2 = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n3 = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        n1.becomeFollower(1, 0L)
        n2.becomeFollower(1, 0L)
        n3.becomeFollower(1, 0L)

        n1.preVote = true
        n2.preVote = true
        n3.preVote = true

        // cause a network partition to isolate node 3
        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2), StateMachine(n3)))
        nt.cut(1, 3)
        nt.cut(2, 3)

        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        var raft = nt.peers[1]!!.raft!!
        assertEquals(LEADER, raft.state)

        raft = nt.peers[2]!!.raft!!
        assertEquals(FOLLOWER, raft.state)

        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        raft = nt.peers[3]!!.raft!!
        assertEquals(PRE_CANDIDATE, raft.state)

        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))
        // check whether the term values are expected
        // a.Term == 3
        // b.Term == 3
        // c.Term == 1
        raft = nt.peers[1]!!.raft!!
        assertEquals(3, raft.term)

        raft = nt.peers[2]!!.raft!!
        assertEquals(3, raft.term)

        raft = nt.peers[3]!!.raft!!
        assertEquals(1, raft.term)

        // check state
        // a == follower
        // b == leader
        // c == pre-candidate
        raft = nt.peers[1]!!.raft!!
        assertEquals(FOLLOWER, raft.state)

        raft = nt.peers[2]!!.raft!!
        assertEquals(LEADER, raft.state)

        raft = nt.peers[3]!!.raft!!
        assertEquals(PRE_CANDIDATE, raft.state)
        log.info { "going to bring back peer 3 and kill peer 2" }
        // recover the network then immediately isolate b which is currently
        // the leader, this is to emulate the crash of b.
        nt.recover()
        nt.cut(2, 1)
        nt.cut(2, 3)

        // call for election
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // do we have a leader?
        val sma = nt.peers[1]!!.raft!!
        val smb = nt.peers[3]!!.raft!!
        if (sma.state != LEADER && smb.state != LEADER) {
            fail("no leader")
        }
    }

    // TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
    // election in next round.
    @Test
    fun testPreVoteWithSplitVote() {
        val n1 = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n2 = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n3 = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        n1.becomeFollower(1, 0L)
        n2.becomeFollower(1, 0L)
        n3.becomeFollower(1, 0L)

        n1.preVote = true
        n2.preVote = true
        n3.preVote = true

        // cause a network partition to isolate node 3
        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2), StateMachine(n3)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // simulate leader down. followers start split vote.
        nt.isolate(1)
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup), buildMessage(3, 3, MessageType.MsgHup)))

        // check whether the term values are expected
        // n2.Term == 3
        // n3.Term == 3
        var raft = nt.peers[2]!!.raft!!
        assertEquals(3, raft.term)

        raft = nt.peers[3]!!.raft!!
        assertEquals(3, raft.term)

        // check state
        // n2 == candidate
        // n3 == candidate
        raft = nt.peers[2]!!.raft!!
        assertEquals(CANDIDATE,raft.state)

        raft = nt.peers[3]!!.raft!!
        assertEquals(CANDIDATE ,raft.state)

        // node 2 election timeout first
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))

        // check whether the term values are expected
        // n2.Term == 4
        // n3.Term == 4
        raft = nt.peers[2]!!.raft!!
        assertEquals(4, raft.term)
        raft = nt.peers[3]!!.raft!!
        assertEquals(4, raft.term)

        // check state
        // n2 == leader
        // n3 == follower
        raft = nt.peers[2]!!.raft!!
        assertEquals(LEADER, raft.state)
        raft = nt.peers[3]!!.raft!!
        assertEquals(FOLLOWER, raft.state)
    }

    // simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
    // n1 is leader with term 2
    // n2 is follower with term 2
    // n3 is partitioned, with term 4 and less log, state is candidate
    fun newPreVoteMigrationCluster() :Network{
        val n1 = newTestRaft(1, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n2 = newTestRaft(2, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())
        val n3 = newTestRaft(3, mutableListOf(1, 2, 3), 10, 1, MemoryRaftStorage())

        n1.becomeFollower(1,0L)
        n2.becomeFollower(1,0L)
        n3.becomeFollower(1,0L)

        n1.preVote = true
        n2.preVote = true

        // We intentionally do not enable PreVote for n3, this is done so in order
        // to simulate a rolling restart process where it's possible to have a mixed
        // version cluster with replicas with PreVote enabled, and replicas without.
        val nt = Network.newNetwork(mutableListOf(StateMachine(n1), StateMachine(n2), StateMachine(n3)))
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgHup)))

        // Cause a network partition to isolate n3.
        nt.isolate(3)
        nt.send(mutableListOf(buildMessage(1, 1, MessageType.MsgProp, mutableListOf(Entry.getDefaultInstance()))))
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        // check state
        // n1.state == StateLeader
        // n2.state == StateFollower
        // n3.state == StateCandidate
        assertEquals(LEADER, n1.state)
        assertEquals(FOLLOWER, n2.state)
        assertEquals(CANDIDATE, n3.state)

        // check term
        // n1.Term == 2
        // n2.Term == 2
        // n3.Term == 4
        assertEquals(2, n1.term)
        assertEquals(2, n2.term)
        assertEquals(4, n3.term)

        // Enable prevote on n3, then recover the network
        n3.preVote = true
        nt.recover()
        return nt
    }

    @Test
    fun testPreVoteMigrationCanCompleteElection() {
        val nt = newPreVoteMigrationCluster()

        // n1 is leader with term 2
        // n2 is follower with term 2
        // n3 is pre-candidate with term 4, and less log
        val n2 = nt.peers[2]!!.raft!!
        val n3 = nt.peers[3]!!.raft!!

        // simulate leader down
        nt.isolate(1)

        // Call for elections from both n2 and n3.
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))

        // check state
        // n2.state == Follower
        // n3.state == PreCandidate
        assertEquals(FOLLOWER, n2.state)
        assertEquals(PRE_CANDIDATE, n3.state)

        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        nt.send(mutableListOf(buildMessage(2, 2, MessageType.MsgHup)))

        if(n2.state != LEADER  && n3.state != LEADER){
            fail("no leader")
        }
    }

    @Test
    fun testPreVoteMigrationWithFreeStuckPreCandidate() {
        val nt = newPreVoteMigrationCluster()

        // n1 is leader with term 2
        // n2 is follower with term 2
        // n3 is pre-candidate with term 4, and less log
        val n1 = nt.peers[1]!!.raft!!
        val n2 = nt.peers[2]!!.raft!!
        val n3 = nt.peers[3]!!.raft!!

        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))

        assertEquals(LEADER, n1.state)
        assertEquals(FOLLOWER, n2.state)
        assertEquals(PRE_CANDIDATE, n3.state)

        // Pre-Vote again for safety
        nt.send(mutableListOf(buildMessage(3, 3, MessageType.MsgHup)))
        assertEquals(LEADER, n1.state)
        assertEquals(FOLLOWER, n2.state)
        assertEquals(PRE_CANDIDATE, n3.state)

        nt.send(mutableListOf(Message.newBuilder().setFrom(1).setTo(3).setType(MessageType.MsgHeartbeat).setTerm(n1.term).build()))

        // Disrupt the leader so that the stuck peer is freed
        assertEquals(FOLLOWER, n1.state)
        assertEquals(n1.term, n3.term)
    }

    // votedWithConfig creates a raft state machine with Vote and Term set
    // to the given value but no log entries (indicating that it voted in
    // the given term but has not received any logs).
    fun votedWithConfig(vote: Long, term: Long, preVote: Boolean): StateMachine {
        val storage = MemoryRaftStorage()
        storage.setHardState(HardState.newBuilder().setVote(vote).setTerm(term).build())
        val cfg = newTestConfig(1, mutableListOf(), 5, 1, storage)
        if (preVote) {
            cfg.preVote = preVote
        }
        val raft = Raft(cfg)
        val sm = StateMachine(raft)
        sm.raft!!.reset(term)
        return sm
    }

    fun entsWithConfig(terms: MutableList<Long>, preVote: Boolean): StateMachine {
        val storage = MemoryRaftStorage()
        terms.forEachIndexed { i, term ->
            storage.append(mutableListOf(ProtoBufUtils.buildEntry((i + 1).toLong(), term)))
        }

        val raft = newTestRaftWithPreVote(1, mutableListOf(), 5, 1, storage, preVote)
        raft.reset(terms[terms.size - 1])
        return StateMachine(raft)
    }

    companion object {

        // setRandomizedElectionTimeout set up the value by caller instead of choosing
        // by system, in some test scenario we need to fill in some expected value to
        // ensure the certainty
        fun setRandomizedElectionTimeout(r: Raft, v: Long) {
            r.randomizedElectionTimeout = v
        }

        fun newTestConfig(id: Long, peers: List<Long>, election: Int, heartbeat: Int, storage: RaftStorage): RaftConfiguration {
            val configuration = RaftConfiguration()
            configuration.id = id
            configuration.peers = peers
            configuration.electionTick = election
            configuration.heartbeatTick = heartbeat
            configuration.raftStorage = storage
            configuration.maxSizePerMsg = Long.MAX_VALUE
            configuration.maxInflightMsgs = 256
            return configuration
        }

        fun newTestRaftWithPreVote(id: Long, peers: List<Long>, election: Int, heartbeat: Int, storage: RaftStorage, preVote: Boolean): Raft {
            val cfg = newTestConfig(id, peers, election, heartbeat, storage)
            cfg.preVote = preVote
            return newTestRaft(id, peers, election, heartbeat, storage)
        }

        fun newTestRaft(id: Long, peers: List<Long>, election: Int, heartbeat: Int, storage: RaftStorage): Raft {
            val config = newTestConfig(id, peers, election, heartbeat, storage)
            return Raft(config)
        }

        fun newTestLearnerRaft(id: Long, peers: List<Long>, learners: List<Long>, election: Int, heartbeat: Int, storage: RaftStorage): Raft {
            val cfg = newTestConfig(id, peers, election, heartbeat, storage)
            cfg.learners = learners
            return Raft(cfg)
        }

        @JvmStatic
        fun readMessages(raft: Raft?): List<RaftProtoBuf.Message> {
            raft?.run {
                val msgs = raft.msgs
                raft.msgs = Lists.newArrayList()
                return msgs
            } ?: return Collections.emptyList()
        }

        // nextEnts returns the appliable entries and updates the applied index
        fun nextEnts(r: Raft, s: RaftStorage): MutableList<RaftProtoBuf.Entry> {
            // Transfer all unstable entries to "stable" storage.
            s.append(r.raftLog.unstableEntries())
            r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm())

            val ents = r.raftLog.nextEnts()
            r.raftLog.appliedTo(r.raftLog.committed)
            return ents
        }

        private fun mustTerm(result: Result<Long>): Long {
            if (result.isFailure) {
                throw RaftException(ErrorCode.getByCode(result.code).desc)
            }

            return result.data
        }
    }

}
