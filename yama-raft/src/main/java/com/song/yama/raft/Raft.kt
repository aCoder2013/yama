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

import com.google.common.base.Joiner
import com.google.common.collect.Lists
import com.google.common.collect.Maps
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.protobuf.ByteString
import com.song.yama.raft.exception.RaftException
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.protobuf.RaftProtoBuf.*
import com.song.yama.raft.utils.ErrorCode
import com.song.yama.raft.utils.Utils
import mu.KotlinLogging
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.RandomUtils
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.function.BiConsumer

private val log = KotlinLogging.logger {}

open class Raft(private val raftConfiguration: RaftConfiguration) {

    var id: Long = 0L

    var term: Long = 0

    var vote: Long = 0

    var readStates = mutableListOf<ReadState>()

    /**
     * the log
     */
    var raftLog: RaftLog

    private val maxInflight: Int

    private val maxMsgSize: Long

    var prs = mutableMapOf<Long, Progress>()

    var learnerPrs = mutableMapOf<Long, Progress>()

    private val matchBuf = ArrayList<Long>()

    var state: StateType = StateType.FOLLOWER //FIXME:default follwer?

    /**
     * isLearner is true if the local raft node is a learner
     */
    var isLearner: Boolean = false

    var votes = HashMap<Long, Boolean>()

    var msgs = Lists.newArrayList<RaftProtoBuf.Message>()

    /**
     * the leader id
     */
    var lead: Long = 0

    /**
     * leadTransferee is id of the leader transfer target when its value is not zero. Follow the procedure defined in
     * raft thesis 3.10.
     */
    var leadTransferee: Long = 0

    /**
     * Only one conf change may be pending (in the log, but not yet applied) at a time. This is enforced via
     * pendingConfIndex, which is set to a value >= the log index of the latest pending configuration change (if any).
     * Config changes are only allowed to be proposed if the leader's applied index is greater than this value.
     */
    var pendingConfIndex: Long = 0

    var readonly: Readonly

    /**
     * number of ticks since it reached last electionTimeout when it is leader or candidate.
     *
     * number of ticks since it reached last electionTimeout or received a valid message from current leader when it is
     * a follower.
     */
    var electionElapsed: Long = 0

    /**
     * number of ticks since it reached last heartbeatTimeout. only leader keeps heartbeatElapsed.
     */
    var heartbeatElapsed: Long = 0

    var checkQuorum: Boolean

    var preVote: Boolean

    val heartbeatTimeout: Long

    val electionTimeout: Long

    /**
     * randomizedElectionTimeout is a random number between
     *
     * [electiontimeout, 2 * electiontimeout -1]. It gets reset when raft changes its state to follower or candidate.
     */
    var randomizedElectionTimeout: Long = 0

    private val disableProposalForwarding: Boolean

    private var step: StepType? = null

    private var tickType: TickType? = null

    private val scheduledExecutorService = ScheduledThreadPoolExecutor(1,
            ThreadFactoryBuilder().setNameFormat("raft-core-scheduled-pool")
                    .setUncaughtExceptionHandler { t, e -> log.error("Uncaught exception :" + t, e) }
                    .build())

    init {
        this.raftConfiguration.validate()
        val raftStorage = raftConfiguration.raftStorage
        this.raftLog = RaftLog(raftStorage, raftConfiguration.maxSizePerMsg)
        val raftState = raftStorage.initialState()
        var peers = raftConfiguration.peers
        var learners = raftConfiguration.learners
        if (raftState.confState.nodesCount > 0 || raftState.confState.learnersCount > 0) {
            if (CollectionUtils.isNotEmpty(peers) || CollectionUtils.isNotEmpty(learners)) {
                throw IllegalStateException(
                        "cannot specify both RaftConfiguration(peers, learners) and ConfState(Nodes, Learners)")
            }
            peers = raftState.confState.nodesList
            learners = raftState.confState.learnersList
        }

        this.id = raftConfiguration.id
        this.lead = 0L
        this.isLearner = false
        this.maxMsgSize = raftConfiguration.maxSizePerMsg
        this.maxInflight = raftConfiguration.maxInflightMsgs
        this.electionTimeout = raftConfiguration.electionTick.toLong()
        this.heartbeatTimeout = raftConfiguration.heartbeatTick.toLong()
        this.checkQuorum = raftConfiguration.isCheckQuorum
        this.preVote = raftConfiguration.preVote
        this.readonly = Readonly(raftConfiguration.readOnlyOption)
        this.disableProposalForwarding = raftConfiguration.isDisableProposalForwarding

        for (peer in peers) {
            this.prs[peer] = Progress(1, Inflight(this.maxInflight))
        }

        for (learner in learners) {
            if (this.prs.containsKey(learner)) {
                throw IllegalStateException(
                        String.format("node %x is in both learner and peer list", learner))
            }
            this.learnerPrs.put(learner, Progress(1, Inflight(this.maxInflight), true))
            if (this.id == learner) {
                this.isLearner = true
            }
        }

        if (Utils.EMPTY_HARD_STATE != raftState.hardState) {
            loadState(raftState.hardState)
        }

        if (raftConfiguration.applied > 0) {
            raftLog.appliedTo(raftConfiguration.applied)
        }

        becomeFollower(this.term, 0)

        log.info(String.format(
                "newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
                this.id, Joiner.on(",").skipNulls().join(nodes()), this.term,
                this.raftLog.committed, this.raftLog.applied, this.raftLog.lastIndex(),
                this.raftLog.lastTerm()))
    }

    fun start() {

    }


    fun hasLeader(): Boolean {
        return this.lead != 0L
    }

    fun softState(): SoftState {
        return SoftState(this.lead, this.state)
    }

    fun hardState(): HardState {
        return HardState.newBuilder().setTerm(this.term).setVote(this.vote)
                .setCommit(this.raftLog.committed).build()
    }

    fun quorum(): Int {
        return this.prs.size / 2 + 1
    }

    fun nodes(): List<Long> {
        val nodes = ArrayList<Long>(this.prs.size)
        this.prs.forEach { id, _ -> nodes.add(id) }
        nodes.sort()
        return nodes
    }

    fun learnerNodes(): List<Long> {
        val nodes = ArrayList<Long>(this.learnerPrs.size)
        this.learnerPrs.forEach { id, _ -> nodes.add(id) }
        nodes.sort()
        return nodes
    }

    fun loadState(state: HardState) {
        if (state.commit < this.raftLog.committed || state.commit > this.raftLog
                        .lastIndex()) {
            throw IllegalStateException(String
                    .format("%x state.commit %d is out of range [%d, %d]", this.id, state.commit,
                            this.raftLog.committed, this.raftLog.lastIndex()))
        }
        this.raftLog.committed = state.commit
        this.term = state.term
        this.vote = state.vote
    }

    fun campaign(campaignType: String) {
        val term: Long
        val voteMsg: MessageType

        if (CampaignType.CAMPAIGN_PRE_ELECTION == campaignType) {
            becomePreCandidate()
            voteMsg = MessageType.MsgPreVote
            term = this.term + 1
        } else {
            becomeCandidate()
            voteMsg = MessageType.MsgVote
            term = this.term
        }

        if (quorum() == poll(this.id, Utils.voteRespMsgType(voteMsg), true)) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster). Advance to the next state.
            if (CampaignType.CAMPAIGN_PRE_ELECTION == campaignType) {
                campaign(CampaignType.CAMPAIGN_ELECTION)
            } else {
                becomeLeader()
            }
            return
        }

        this.prs.keys.stream()
                .filter { id -> id != this.id }
                .forEach { id ->
                    log.info("{} [logterm: {}, index: {}] sent {} request to {} at term {}",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), voteMsg, id, this.term)
                    val builder = Message.newBuilder()
                            .setTo(id!!)
                            .setTerm(term)
                            .setType(voteMsg)
                            .setIndex(this.raftLog.lastIndex())
                            .setLogTerm(this.raftLog.lastTerm())
                    if (CampaignType.CAMPAIGN_TRANSFER == campaignType) {
                        builder.context = ByteString.copyFrom(campaignType.toByteArray())
                    }
                    send(builder)
                }
    }


    fun becomeFollower(term: Long, lead: Long) {
        this.step = StepType.StepFollower
        reset(term)
        this.tickType = TickType.Election
        this.lead = lead
        this.state = StateType.FOLLOWER

        log.info(String.format("%x became follower at term %d", this.id, this.term))
    }

    fun becomeCandidate() {
        if (state == StateType.LEADER) {
            throw IllegalStateException("invalid transition [leader -> candidate]")
        }

        this.step = StepType.StepCandidate
        reset(this.term + 1)
        this.tickType = TickType.Election
        this.vote = this.id
        this.state = StateType.CANDIDATE
        log.info(String.format("%x became candidate at term %d", this.id, this.term))
    }

    fun becomePreCandidate() {
        if (this.state == StateType.LEADER) {
            throw IllegalStateException("invalid transition [leader -> pre-candidate]")
        }

        this.step = StepType.StepCandidate
        this.votes = HashMap(16)
        this.tickType = TickType.Election
        this.state = StateType.PRE_CANDIDATE
        log.info(String.format("%x became pre-candidate at term %d", this.id, this.term))
    }

    fun becomeLeader() {
        if (state == StateType.FOLLOWER) {
            throw IllegalStateException("invalid transition [follower -> leader]")
        }

        this.step = StepType.StepLeader
        reset(this.term)
        this.tickType = TickType.Heartbeat
        this.lead = this.id
        this.state = StateType.LEADER

        // Conservatively set the pendingConfIndex to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        pendingConfIndex = raftLog.lastIndex()

        appendEntry(mutableListOf(Entry.newBuilder().build()))
        log.info {
            "${this.id} became leader at term ${this.term}"
        }
    }

    /// Sets the vote of `id` to `vote`.
    ///
    /// Returns the number of votes for the `id` currently.
    fun poll(id: Long, messageType: MessageType, vote: Boolean): Int {
        if (vote) {
            log.info("{} received {} from {} at term {}", this.id, messageType, id, this.term)
        } else {
            log.info("{} received {} rejection from {} at term {}", this.id, messageType, id, this.term)
        }
        if (!this.votes.containsKey(id)) {
            this.votes[id] = vote
        }
        var granted = 0
        for (vv in this.votes.values) {
            if (vv) {
                granted++
            }
        }
        return granted
    }


    fun step(message: Message): ErrorCode {
        // Handle the message term, which may result in our stepping down to a follower.

        val messageType = message.type
        if (message.term == 0L) {
            // local message
        } else if (message.term > this.term) {
            if (messageType == MessageType.MsgVote || messageType == MessageType.MsgPreVote) {
                val force = message.context.toStringUtf8() == CampaignType.CAMPAIGN_TRANSFER
                val inLease = this.checkQuorum && this.lead != 0L && this.electionElapsed < this.electionTimeout
                if (!force && inLease) {
                    // If a server receives a RequestVote request within the minimum election timeout
                    // of hearing from a current leader, it does not update its term or grant its vote
                    log.info(
                            "{} [logterm: %{}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: lease is not expired (remaining ticks: {})",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, messageType,
                            message.from, message.logTerm, message.index, message.term,
                            this.electionTimeout - this.electionElapsed)
                    return ErrorCode.OK
                }
            }

            if (messageType == MessageType.MsgPreVote) {
                // Never change our term in response to a PreVote
            } else if (messageType == MessageType.MsgPreVoteResp && !message.reject) {
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                log.info("{} [term: {}] received a {} message with higher term from {} [term: {}]",
                        this.id, this.term, messageType, message.from, message.term)
                if (messageType == MessageType.MsgApp || messageType == MessageType.MsgHeartbeat
                        || messageType == MessageType.MsgSnap) {
                    becomeFollower(message.term, message.from)
                } else {
                    becomeFollower(message.term, 0)
                }
            }
        } else if (message.term < this.term) {
            if ((this.checkQuorum || this.preVote) && (messageType == MessageType.MsgHeartbeat || messageType == MessageType.MsgApp)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a
                // higher term, but if checkQuorum is true we may not advance the term on
                // MsgVote and must generate other messages to advance the term. The net
                // result of these two features is to minimize the disruption caused by
                // nodes that have been removed from the cluster's configuration: a
                // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                // disruptive term increases, by notifying leader of this node's activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                //r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
                send(Message.newBuilder().setTo(message.from).setType(MessageType.MsgAppResp))
            } else if (messageType == MessageType.MsgPreVote) {
                // Before Pre-Vote enable, there may have candidate with higher term,
                // but less log. After update to Pre-Vote, the cluster may deadlock if
                // we drop messages with a lower term.
                log.info(
                        "{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, messageType,
                        message.from, message.logTerm, message.index, message.term)
                send(Message.newBuilder().setTo(message.from).setTerm(this.term)
                        .setType(MessageType.MsgPreVoteResp).setReject(true))
            } else {
                // ignore other cases
                log.debug("{} [term: {}] ignored a {} message with lower term from {} [term: {}]",
                        this.id, this.term, messageType, message.from, message.term)
            }
            return ErrorCode.OK
        }

        if (messageType == MessageType.MsgHup) {
            if (this.state != StateType.LEADER) {
                val result = this.raftLog
                        .slice(this.raftLog.applied + 1, this.raftLog.committed + 1, Utils.NO_LIMIT)
                if (result.isFailure) {
                    throw RuntimeException(
                            "unexpected error getting unapplied entries :" + result.code + "," + result.message)
                }

                val n = numOfPendingConf(result.data)
                if (n != 0 && this.raftLog.committed > this.raftLog.applied) {
                    log.warn(
                            "{} cannot campaign at term {} since there are still {} pending configuration changes to apply",
                            this.id, this.term, n)
                    return ErrorCode.OK
                }

                log.info("{} is starting a new election at term {}", this.id, this.term)

                if (this.preVote) {
                    campaign(CampaignType.CAMPAIGN_PRE_ELECTION)
                } else {
                    campaign(CampaignType.CAMPAIGN_ELECTION)
                }
            } else {
                log.info("{} ignoring MsgHup because already leader", this.id)
            }
        } else if (messageType == MessageType.MsgVote || messageType == MessageType.MsgPreVote) {
            if (this.isLearner) {
                // TODO: learner may need to vote, in case of node down when confchange.
                log.info(
                        "{} [logterm: {}, index: {}, vote: {}] ignored {} from {} [logterm: {}, index: {}] at term {}: learner can not vote",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, message.type,
                        message.from, message.logTerm, message.index,
                        this.term)
                return ErrorCode.OK
            }

            // We can vote if this is a repeat of a vote we've already cast...
            val canVote = this.vote == message.from
                    // ...we haven't voted and we don't think there's a leader yet in this term...
                    || (this.vote == Utils.INVALID_ID && this.lead == Utils.INVALID_ID)
                    // ...or this is a PreVote for a future term...
                    || (messageType == MessageType.MsgPreVote && message.term > this.term)
            // ...and we believe the candidate is up to date.
            if (canVote && this.raftLog.isUpToDate(message.index, message.logTerm)) {
                log.info("{} [logterm: {}, index: {}, vote: {}] cast {} for {} [logterm: {}, index: {}] at term {}",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, message.type,
                        message.from,
                        message.logTerm, message.index, this.term)

                // When responding to Msg{Pre,}Vote messages we include the term
                // from the message, not the local term. To see why consider the
                // case where a single node was previously partitioned away and
                // it's local term is now of date. If we include the local term
                // (recall that for pre-votes we don't update the local term), the
                // (pre-)campaigning node on the other end will proceed to ignore
                // the message (it ignores all out of date messages).
                // The term in the original message and current local term are the
                // same in the case of regular votes, but different for pre-votes.
                send(Message.newBuilder()
                        .setTo(message.from)
                        .setTerm(message.term)
                        .setType(Utils.voteRespMsgType(messageType)))
                if (message.type == MessageType.MsgVote) {
                    // Only record real votes.
                    this.electionElapsed = 0
                    this.vote = message.from
                }
            } else {
                log.info(
                        "{} [logterm: {}, index: {}, vote: {}] rejected {} from {} [logterm: {}, index: {}] at term {}",
                        this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, message.type,
                        message.from, message.logTerm, message.index,
                        this.term)
                send(Message.newBuilder()
                        .setTo(message.from)
                        .setTerm(this.term)
                        .setType(Utils.voteRespMsgType(messageType))
                        .setReject(true))
            }
        } else {
            when (this.state) {
                StateType.PRE_CANDIDATE, StateType.CANDIDATE -> {
                    return stepCandidate(message)
                }
                StateType.FOLLOWER -> {
                    return stepFollower(message)
                }
                StateType.LEADER -> {
                    return stepLeader(message)
                }
            }
        }
        return ErrorCode.OK
    }

    open fun stepLeader(m: RaftProtoBuf.Message): ErrorCode {

        // These message types do not require any progress for m.From.
        when (m.type) {
            MessageType.MsgBeat -> {
                bcastHeartbeat()
                return ErrorCode.OK
            }
            MessageType.MsgCheckQuorum -> {
                if (!checkQuorumActive()) {
                    log.warn { "${this.id} stepped down to follower since quorum is not active" }
                    becomeFollower(this@Raft.term, 0)
                }
                return ErrorCode.OK
            }
            MessageType.MsgProp -> {
                if (m.entriesList.isNullOrEmpty()) {
                    throw IllegalArgumentException("${this@Raft.id} stepped empty MsgProp")
                }

                if (!this@Raft.prs.containsKey(this@Raft.id)) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return ErrorCode.ErrProposalDropped
                }

                if (leadTransferee != 0L) {
                    log.info {
                        "${this@Raft.id} [term ${this@Raft.term}] transfer leadership to ${this@Raft.leadTransferee} is in progress; dropping proposal"
                    }
                    return ErrorCode.ErrProposalDropped
                }


                val entries = m.entriesList.toMutableList()
                m.entriesList.forEachIndexed { index, entry ->
                    if (entry.type == RaftProtoBuf.EntryType.EntryConfChange) {
                        if (this@Raft.pendingConfIndex > this@Raft.raftLog.applied) {
                            log.info { "propose conf $entry ignored since pending unapplied configuration [index ${this@Raft.pendingConfIndex}, applied ${this@Raft.raftLog.applied}]" }
                            entries[index] = RaftProtoBuf.Entry.newBuilder().setType(RaftProtoBuf.EntryType.EntryNormal).build()
                        } else {
                            this@Raft.pendingConfIndex = this@Raft.raftLog.lastIndex() + index + 1
                        }
                    }
                }

                appendEntry(entries)
                bcastAppend()
                return ErrorCode.OK
            }

            RaftProtoBuf.MessageType.MsgReadIndex -> {
                if (quorum() > 1) {
                    val zeroTermOnErrCompacted = raftLog.zeroTermOnErrCompacted(raftLog.term(this@Raft.raftLog.committed).data, null)
                    if (zeroTermOnErrCompacted != this.term) {
                        // Reject read only request when this leader has not committed any log entry at its term.
                        return ErrorCode.OK
                    } else {
                        // thinking: use an interally defined context instead of the user given context.
                        // We can express this in terms of the term and index instead of a user-supplied value.
                        // This would allow multiple reads to piggyback on the same message.
                        when (readonly.option) {
                            ReadOnlyOption.READ_ONLY_SAFE -> {
                                readonly.addRequest(raftLog.committed, m)
                                bcastHeartbeatWithCtx(m.entriesList[0].data.toByteArray())
                            }
                            ReadOnlyOption.READ_ONLY_LEASE_BASED -> {
                                val ri = raftLog.committed
                                if (m.from == 0L || m.from == this@Raft.id) {
                                    readStates.add(ReadState(raftLog.committed, m.entriesList[0].data.toByteArray()))
                                } else {
                                    send(RaftProtoBuf.Message.newBuilder()
                                            .setTo(m.from)
                                            .setType(RaftProtoBuf.MessageType.MsgReadIndexResp)
                                            .setIndex(ri)
                                            .addAllEntries(m.entriesList))
                                }
                            }
                        }
                    }
                } else {
                    readStates.add(ReadState(this@Raft.raftLog.committed, m.entriesList[0].data.toByteArray()))
                }
                return ErrorCode.OK
            }
            else -> {
                //do nothing
            }
        }

        // All other message types require a progress for m.From (pr).
        val pr = getProgress(m.from)
        if (pr == null) {
            log.info {
                "${this.id} no progress available for ${m.from}"
            }
            return ErrorCode.OK
        }

        when (m.type) {
            MessageType.MsgAppResp -> {
                pr.isRecentActive = true
                if (m.reject) {
                    log.info {
                        "$id received msgApp rejection(lastindex: ${m.rejectHint}) from ${m.from} for index ${m.index}"
                    }
                    if (pr.maybeDecrTo(m.index, m.rejectHint)) {
                        log.info {
                            "$id decreased progress of ${m.from} to $pr"
                        }
                        if (pr.state == Progress.ProgressStateType.ProgressStateReplicate) {
                            pr.becomeProbe()
                        }
                        sendAppend(m.from)
                    }
                } else {
                    val oldPaused = pr.isPaused()
                    if (pr.maybeUpdate(m.index)) {
                        if (pr.state == Progress.ProgressStateType.ProgressStateProbe) {
                            pr.becomeReplicate()
                        } else if (pr.state == Progress.ProgressStateType.ProgressStateSnapshot && pr.needSnapshotAbort()) {
                            log.info { String.format("%x snapshot aborted, resumed sending replication messages to %x [%s]", this.id, m.from, pr) }
                            pr.becomeProbe()
                        } else if (pr.state == Progress.ProgressStateType.ProgressStateReplicate) {
                            pr.ins.freeTo(m.index)
                        }

                        if (maybeCommit()) {
                            bcastAppend()
                        } else if (oldPaused) {
                            // If we were paused before, this node may be missing the
                            // latest commit index, so send it.
                            sendAppend(m.from)
                        }

                        // We've updated flow control information above, which may
                        // allow us to send multiple (size-limited) in-flight messages
                        // at once (such as when transitioning from probe to
                        // replicate, or when freeTo() covers multiple messages). If
                        // we have more entries to send, send as many messages as we
                        // can (without sending empty messages for the commit index)
                        while (maybeSendAppend(m.from, false)) {

                        }

                        // Transfer leadership is in progress.
                        if (m.from == this.leadTransferee && pr.match == this.raftLog.lastIndex()) {
                            log.info { String.format("%x sent MsgTimeoutNow to %x after received MsgAppResp", this.id, m.from) }
                            sendTimeoutNow(m.from)
                        }
                    }
                }
            }
            MessageType.MsgHeartbeatResp -> {
                pr.isRecentActive = true
                pr.resume()

                // free one slot for the full inflights window to allow progress.
                if (pr.state == Progress.ProgressStateType.ProgressStateReplicate && pr.ins.isFull()) {
                    pr.ins.freeFirstOne()
                }

                if (pr.match < this.raftLog.lastIndex()) {
                    sendAppend(m.from)
                }

                if (this.readonly.option != ReadOnlyOption.READ_ONLY_SAFE || m.context.isEmpty) {
                    return ErrorCode.OK
                }

                val ackCount = this.readonly.recvAck(m)
                if (ackCount < quorum()) {
                    return ErrorCode.OK
                }

                val rss = this.readonly.advance(m)
                rss.forEach { rs ->
                    val req = rs.req
                    if (req.from == 0L || req.from == this.id) { // from local member
                        this.readStates.add(ReadState(rs.index, req.getEntries(0).data.toByteArray()))
                    } else {
                        send(Message.newBuilder().setTo(req.from).setType(MessageType.MsgReadIndexResp).setIndex(rs.index).addAllEntries(req.entriesList))
                    }
                }
            }
            MessageType.MsgSnapStatus -> {
                if (pr.state == Progress.ProgressStateType.ProgressStateSnapshot) {
                    return ErrorCode.OK
                }

                if (!m.reject) {
                    pr.becomeProbe()
                    log.info { "${this.id} snapshot succeeded, resumed sending replication messages to ${m.from} [$pr]" }
                } else {
                    pr.snapshotFailure()
                    pr.becomeProbe()
                    log.info { "${this.id} snapshot failed, resumed sending replication messages to $m.from [$pr]" }
                }

                // If snapshot finish, wait for the msgAppResp from the remote node before sending
                // out the next msgApp.
                // If snapshot failure, wait for a heartbeat interval before next try
                pr.pause()
            }
            MessageType.MsgUnreachable -> {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgApp is lost.
                if (pr.state == Progress.ProgressStateType.ProgressStateReplicate) {
                    pr.becomeProbe()
                }
                log.info { "${this.id} failed to send message to ${m.from} because it is unreachable [$pr]" }
            }
            MessageType.MsgTransferLeader -> {
                if (pr.isLearner) {
                    log.info { "${this.id} is learner. Ignored transferring leadership" }
                    return ErrorCode.OK
                }
                val leadTransferee = m.from
                val lastLeadTransferee = this.leadTransferee
                if (lastLeadTransferee != 0L) {
                    if (lastLeadTransferee == leadTransferee) {
                        log.info {
                            String.format("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x", this.id, this.term, leadTransferee, leadTransferee)
                        }
                        return ErrorCode.OK
                    }

                    abortLeaderTransfer()
                    log.info { String.format("%x [term %d] abort previous transferring leadership to %x", this.id, this.term, lastLeadTransferee) }
                }

                if (leadTransferee == this.id) {
                    log.info { "${this.id} is already leader. Ignored transferring leadership to self" }
                    return ErrorCode.OK
                }

                // Transfer leadership to third party.
                log.info { "${this.id} [term ${this.term}] starts to transfer leadership to $leadTransferee" }
                // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
                this.electionElapsed = 0
                this.leadTransferee = leadTransferee
                if (pr.match == this.raftLog.lastIndex()) {
                    sendTimeoutNow(leadTransferee)
                    log.info { String.format("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", this.id, leadTransferee, leadTransferee) }
                } else {
                    sendAppend(leadTransferee)
                }
            }
            else -> {
                //do nothing
            }
        }
        return ErrorCode.OK
    }

    /**
     *  sendAppend sends an append RPC with new entries (if any) and the
     *  current commit index to the given peer.
     */
    fun sendAppend(to: Long) {
        maybeSendAppend(to, true)
    }

    /**
     * maybeSendAppend sends an append RPC with new entries to the given peer,
     * if necessary. Returns true if a message was sent. The sendIfEmpty
     * argument controls whether messages with no entries will be sent
     * ("empty" messages are useful to convey updated Commit indexes, but
     * are undesirable when we're sending multiple messages in a batch).
     */
    fun maybeSendAppend(to: Long, sendIfEmpty: Boolean): Boolean {
        val pr = getProgress(to)
        if (pr == null || pr.isPaused()) {
            return false
        }

        val m = Message.newBuilder()
        m.to = to

        val term = this.raftLog.term(pr.next - 1)
        val ents = this.raftLog.entries(pr.next, this.maxMsgSize)

        if (ents.data.isNullOrEmpty() && !sendIfEmpty) {
            return false
        }

        if (term.isFailure || ents.isFailure) {
            // send snapshot if we failed to get term or entries
            if (!pr.isRecentActive) {
                log.info {
                    "ignore sending snapshot to $to since it is not recently active"
                }
                return false
            }

            m.type = MessageType.MsgSnap
            val result = this.raftLog.snapshot()
            if (result.isFailure) {
                if (result.code == ErrorCode.ErrSnapshotTemporarilyUnavailable.code) {
                    log.info {
                        "${this.id} failed to send snapshot to $to because snapshot is temporarily unavailable"
                    }
                    return false
                }

                throw RaftException("Unknown exception : " + result.code)
            }

            val snapshot = result.data
            if (Utils.isEmptySnap(snapshot)) {
                throw RaftException("need non-empty snapshot")
            }

            m.snapshot = snapshot
            val sindex = snapshot.metadata.index
            val sterm = snapshot.metadata.term
            log.info {
                "${this.id} [firstindex: ${raftLog.firstIndex()}, commit: ${this.raftLog.committed}]" +
                        " sent snapshot[index: $sindex, term: $sterm] to $to [$pr]"
            }
            pr.becomeSnapshot(sindex)
            log.info {
                "${this.id} paused sending replication messages to ${to} [$pr]"
            }
        } else {
            m.type = MessageType.MsgApp
            m.index = pr.next - 1
            m.logTerm = term.data
            m.addAllEntries(ents.data)
            m.commit = this.raftLog.committed

            if (m.entriesList.isNotEmpty()) {
                when {
                    // optimistically increase the next when in ProgressStateReplicate
                    pr.state == Progress.ProgressStateType.ProgressStateReplicate -> {
                        val last = m.entriesList[m.entriesList.size - 1].index
                        pr.optimisticUpdate(last)
                        pr.ins.add(last)
                    }
                    pr.state == Progress.ProgressStateType.ProgressStateProbe -> {
                        pr.pause()
                    }
                    else -> {
                        throw RaftException("${this.id} is sending append in unhandled state ${pr.state}")
                    }
                }
            }
        }
        send(m)
        return true
    }

    /**
     * stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
     * whether they respond to MsgVoteResp or MsgPreVoteResp.
     */
    open fun stepCandidate(m: Message): ErrorCode {
        // Only handle vote responses corresponding to our candidacy (while in
        // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
        // our pre-candidate state).
        val myVoteRespType = if (this.state == StateType.PRE_CANDIDATE) {
            MessageType.MsgPreVoteResp
        } else {
            MessageType.MsgVoteResp
        }

        when {
            m.type == MessageType.MsgProp -> {
                log.info {
                    "${this.id} no leader at term ${this.term}; dropping proposal"
                }
                return ErrorCode.ErrProposalDropped
            }
            m.type == MessageType.MsgApp -> {
                becomeFollower(m.term, m.from)  // always m.Term == r.Term
                handleAppendEntries(m)
            }
            m.type == MessageType.MsgHeartbeat -> {
                becomeFollower(m.term, m.from)  // always m.Term == r.Term
                handleHeartbeat(m)
            }
            m.type == MessageType.MsgSnap -> {
                becomeFollower(m.term, m.from)  // always m.Term == r.Term
                handleSnapshot(m)
            }
            m.type == myVoteRespType -> {
                val gr = poll(m.from, m.type, !m.reject)
                log.info {
                    "${this.id} [quorum:${quorum()}] has received $gr ${m.type} votes and ${this.votes.size - gr} vote rejections"
                }

                val quorum = quorum()
                when (quorum) {
                    gr -> {
                        if (state == StateType.PRE_CANDIDATE) {
                            campaign(CampaignType.CAMPAIGN_ELECTION)
                        } else {
                            becomeLeader()
                            bcastAppend()
                        }
                    }
                    (this.votes.size - gr) -> {
                        // pb.MsgPreVoteResp contains future term of pre-candidate
                        // m.Term > r.Term; reuse r.Term
                        becomeFollower(this.term, 0)
                    }
                }
            }

            m.type == MessageType.MsgTimeoutNow -> {
                log.info {
                    "${this.id} [term ${this.term} state ${this.state}] ignored MsgTimeoutNow from ${m.from}"
                }
            }
        }
        return ErrorCode.OK
    }

    open fun stepFollower(m: Message): ErrorCode {
        val type = m.type
        if (type == MessageType.MsgProp) {
            if (this.lead == 0L) {
                log.info {
                    "${this.id} no leader at term ${this.term}; dropping proposal"
                }
                return ErrorCode.ErrProposalDropped
            } else if (this.disableProposalForwarding) {
                log.info {
                    "${this.id} not forwarding to leader ${this.lead} at term ${this.term}; dropping proposal"
                }
                return ErrorCode.ErrProposalDropped
            }
            send(m.toBuilder().setTo(this.lead))
        } else if (type == MessageType.MsgApp) {
            this.electionElapsed = 0
            this.lead = m.from
            handleAppendEntries(m)
        } else if (type == MessageType.MsgHeartbeat) {
            this.electionElapsed = 0
            this.lead = m.from
            handleHeartbeat(m)
        } else if (type == MessageType.MsgSnap) {
            this.electionElapsed = 0
            this.lead = m.from
            handleSnapshot(m)
        } else if (type == MessageType.MsgTransferLeader) {
            if (this.lead == 0L) {
                log.info { "${this.id} no leader at term ${this.term}; dropping leader transfer msg" }
            }
            send(m.toBuilder().setTo(this.lead))
        } else if (type == MessageType.MsgTimeoutNow) {
            if (promotable()) {
                log.info {
                    "${this.id} [term ${this.term}] received MsgTimeoutNow from ${m.from} and starts an election to get leadership."
                }
                // Leadership transfers never use pre-vote even if r.preVote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
                campaign(CampaignType.CAMPAIGN_TRANSFER)
            } else {
                log.info {
                    "${this.id} received MsgTimeoutNow from ${m.from} but is not promotable"
                }
            }
        } else if (type == MessageType.MsgReadIndex) {
            if (this.lead == 0L) {
                log.info {
                    "${this.id} no leader at term ${this.term}; dropping index reading msg"
                }
                return ErrorCode.OK
            }
            send(m.toBuilder()
                    .setTo(this.lead))
        } else if (type == MessageType.MsgReadIndexResp) {
            if (m.entriesList.size != 1) {
                log.error {
                    "${this.id} invalid format of MsgReadIndexResp from ${m.from}, entries count: ${m.entriesCount}"
                }
                return ErrorCode.OK
            }
            this.readStates.add(ReadState(m.index, m.entriesList[0].data.toByteArray()))
        }
        return ErrorCode.OK
    }

    fun handleAppendEntries(m: Message) {
        if (m.index < this.raftLog.committed) {
            send(Message.newBuilder()
                    .setTo(m.from)
                    .setType(MessageType.MsgAppResp)
                    .setIndex(this.raftLog.committed))
            return
        }

        val result = this.raftLog.maybeAppend(m.index, m.logTerm, m.commit, m.entriesList)
        if (result.isSuccess) {
            send(Message.newBuilder()
                    .setTo(m.from)
                    .setType(MessageType.MsgAppResp)
                    .setIndex(result.data))
        } else {
            log.info {
                "${this.id} [logterm: ${this.raftLog.zeroTermOnErrCompacted(this.raftLog.term(m.index).data, null)}," +
                        " index: ${m.index}] rejected msgApp [logterm: ${m.logTerm}, index: ${m.index}] from ${m.from}"
            }
            send(Message.newBuilder()
                    .setTo(m.from)
                    .setType(MessageType.MsgAppResp)
                    .setIndex(m.index)
                    .setReject(true)
                    .setRejectHint(this.raftLog.lastIndex()))
        }
    }

    fun handleHeartbeat(m: Message) {
        this.raftLog.commitTo(m.commit)
        send(Message.newBuilder()
                .setTo(m.from)
                .setType(MessageType.MsgHeartbeatResp)
                .setContext(m.context))
    }

    fun handleSnapshot(m: Message) {
        val sindex = m.snapshot.metadata.index
        val stream = m.snapshot.metadata.term
        if (restore(m.snapshot)) {
            log.info {
                "${this.id} [commit: ${this.raftLog.committed}] restored snapshot [index: $sindex, term: $stream]"
            }
            send(Message.newBuilder()
                    .setTo(m.from)
                    .setType(MessageType.MsgAppResp)
                    .setIndex(this.raftLog.lastIndex()))
        } else {
            log.info {
                "${this.id} [commit: ${this.raftLog.committed}] ignored snapshot [index: $sindex, term: $stream]"
            }
            send(Message.newBuilder()
                    .setTo(m.from)
                    .setType(MessageType.MsgAppResp)
                    .setIndex(this.raftLog.committed))
        }
    }

    /**
     *restore recovers the state machine from a snapshot. It restores the log and the
     * configuration of state machine.
     */
    fun restore(s: Snapshot): Boolean {
        if (s.metadata.index <= this.raftLog.committed) {
            return false
        }

        if (this.raftLog.matchTerm(s.metadata.index, s.metadata.term)) {
            log.info {
                "${this.id} [commit: ${this.raftLog.committed}, lastindex: ${this.raftLog.lastIndex()}, " +
                        "lastterm: ${this.raftLog.lastTerm()}] fast-forwarded commit to snapshot [index: ${s.metadata.index}, term: ${s.metadata.term}]"
            }

            this.raftLog.commitTo(s.metadata.index)
            return false
        }

        if (!this.isLearner) {
            s.metadata.confState.learnersList.forEach { id: Long ->
                if (this.id == id) {
                    log.error {
                        "${this.id} can't become learner when restores snapshot [index: ${s.metadata.index}, term: ${s.metadata.term}]"
                    }
                    return false
                }
            }
        }

        log.info {
            "${this.id} [commit: ${this.raftLog.committed}, lastindex: ${this.raftLog.lastIndex()}, lastterm: ${this.raftLog.lastTerm()}]" +
                    " starts to restore snapshot [index: ${s.metadata.index}, term: ${s.metadata.term}]"
        }

        this.raftLog.restore(s)
        this.prs = mutableMapOf()
        this.learnerPrs = mutableMapOf()
        restoreNode(s.metadata.confState.nodesList, false)
        restoreNode(s.metadata.confState.learnersList, true)
        return true
    }

    fun restoreNode(nodes: List<Long>, isLearner: Boolean) {
        nodes.forEach { id: Long ->
            var match = 0L
            var next = this.raftLog.lastIndex() + 1

            if (id == this.id) {
                match = next - 1
                this.isLearner = isLearner
            }
            setProgress(id, match, next, isLearner)
            log.info {
                "${this.id} restored progress of $id [${getProgress(id)}]"
            }
        }

    }


    /**
     * send persists state to stable storage and then sends to its mailbox
     */
    private fun send(message: Message.Builder) {
        message.from = this.id
        if (message.type == MessageType.MsgVote
                || message.type == MessageType.MsgPreVote
                || message.type == MessageType.MsgVoteResp
                || message.type == MessageType.MsgPreVoteResp) {
            if (message.term == 0L) {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                throw IllegalArgumentException("term should be set when sending :" + message.type)
            }
        } else {
            if (message.term != 0L) {
                throw IllegalArgumentException(
                        String.format("term should not be set when sending %s (was %d)", message.type,
                                message.term))
            }

            // do not attach term to MsgProp, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if (message.type != MessageType.MsgProp && message.type != MessageType.MsgReadIndex) {
                message.term = this.term
            }
        }
        this.msgs.add(message.build())
    }


    /**
     * maybeCommit attempts to advance the commit index. Returns true if the commit index changed (in which case the
     * caller should call r.bcastAppend).
     */
    fun maybeCommit(): Boolean {
        // Preserving matchBuf across calls is an optimization

        val mis = ArrayList<Long>(this.prs.size)
        prs.forEach { _, progress ->
            mis.add(progress.match)
        }
        mis.sort()
        val mci = mis[mis.size - quorum()]
        return this.raftLog.maybeCommit(mci, this.term)
    }


    fun reset(term: Long) {
        if (this.term != term) {
            this.term = term
            this.vote = Utils.INVALID_ID
        }

        this.lead = 0

        this.electionElapsed = 0
        this.heartbeatElapsed = 0
        resetRandomizedElectionTimeout()

        abortLeaderTransfer()

        this.votes = Maps.newHashMapWithExpectedSize(16)
        forEachProgress(BiConsumer { id, progress ->
            run {
                progress.next = this.raftLog.lastIndex() + 1
                progress.ins = Inflight(this.maxInflight)
                progress.isLearner = this.isLearner
                if (id == this.id) {
                    progress.match = this.raftLog.lastIndex()
                }
            }
        })

        this.pendingConfIndex = 0
        this.readonly = Readonly(this.readonly.option)
    }

    fun appendEntry(entries: List<Entry>) {
        log.info { "appendEntry,${entries[0].type}" }
        var lastIndex = raftLog.lastIndex()

        val ents = MutableList<Entry>(entries.size) { index: Int ->
            entries[index].toBuilder()
                    .setTerm(this.term)
                    .setIndex(lastIndex + 1 + index)
                    .build()
        }
        // use latest "last" index after truncate/append
        lastIndex = raftLog.append(ents)
        getProgress(this.id)!!.maybeUpdate(lastIndex)
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        maybeCommit()
    }

    fun getProgress(id: Long): Progress? {
        return if (this.prs.containsKey(id)) {
            this.prs[id]
        } else this.learnerPrs[id]
    }


    fun tick() {
        when (this.tickType) {
            Raft.TickType.Election -> tickElection()
            Raft.TickType.Heartbeat -> tickHeartbeat()
            else -> throw IllegalStateException("Unknown tick function type")
        }
    }

    /**
     * tickElection is run by followers and candidates after r.electionTimeout
     */
    fun tickElection() {
        this.electionElapsed++

        if (promotable() && pastElectionTimeout()) {
            this.electionElapsed = 0

            step(Message.newBuilder().setFrom(this.id)
                    .setType(MessageType.MsgHup).build())
        }
    }

    /**
     * tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout
     */
    fun tickHeartbeat() {
        this.heartbeatElapsed++
        this.electionElapsed++

        if (electionElapsed >= electionTimeout) {
            this.electionElapsed = 0
            if (checkQuorum) {
                step(Message.newBuilder().setFrom(this.id).setType(MessageType.MsgCheckQuorum)
                        .build())
            }

            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (state == StateType.LEADER && leadTransferee != 0L) {
                abortLeaderTransfer()
            }
        }

        if (state != StateType.LEADER) {
            return
        }

        if (heartbeatElapsed >= heartbeatTimeout) {
            step(Message.newBuilder().setFrom(this.id).setType(MessageType.MsgBeat).build())
        }
    }


    /**
     * promotable indicates whether state machine can be promoted to leader, which is true when its own id is in
     * progress list
     */
    fun promotable(): Boolean {
        return this.prs.containsKey(this.id)
    }

    fun addNode(id: Long) {
        addNodeOrLearnerNode(id, false)
    }

    fun addLearner(id: Long) {
        addNodeOrLearnerNode(id, true)
    }


    private fun addNodeOrLearnerNode(id: Long, isLearner: Boolean) {
        var progress = getProgress(id)
        if (progress == null) {
            setProgress(id, 0, this.raftLog.lastIndex() + 1, isLearner)
        } else {
            if (isLearner && !progress.isLearner) {
                log.info(String.format(
                        "%x ignored addLearner: do not support changing %x from raft peer to learner.",
                        this.id, id))
                return
            }

            if (isLearner == progress.isLearner) {
                // Ignore any redundant addNode calls (which can happen because the
                // initial bootstrapping entries are applied twice).
                return
            }

            this.learnerPrs.remove(id)
            progress.isLearner = false
            this.prs[id] = progress
        }


        if (this.id == id) {
            this.isLearner = isLearner
        }

        // When a node is first added, we should mark it as recently active.
        // Otherwise, CheckQuorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        progress = getProgress(id)
        progress!!.isRecentActive = true
    }

    fun removeNode(id: Long) {
        delProgress(id)

        // do not try to commit or abort transferring if there is no nodes in the cluster.
        if (this.prs.isEmpty() && this.learnerPrs.isEmpty()) {
            return
        }

        if (maybeCommit()) {
            bcastAppend()
        }
        if (this.state == StateType.LEADER && leadTransferee == id) {
            abortLeaderTransfer()
        }
    }

    fun setProgress(id: Long, match: Long, next: Long, isLearner: Boolean) {
        if (!isLearner) {
            this.learnerPrs.remove(id)
            this.prs.put(id, Progress(next, match, Inflight(this.maxInflight)))
            return
        }

        if (this.prs.containsKey(id)) {
            throw IllegalStateException(
                    String.format("%x unexpected changing from voter to learner for %x", this.id, id))
        }

        this.learnerPrs
                .put(id, Progress(next, match, Inflight(this.maxInflight), true))
    }

    fun bcastAppend() {
        log.info {
            "bcastAppend"
        }
        forEachProgress(BiConsumer { id, _ ->
            if (id == this.id) {
                log.info { "skip send to myself" }
            } else {
                log.info {
                    "Send message to $id"
                }
                sendAppend(id)
            }
        })
    }

    fun bcastHeartbeat() {
        val lastCtx = readonly.lastPendingRequestCtx()
        if (lastCtx.isBlank()) {
            bcastHeartbeatWithCtx(ByteArray(0))
        } else {
            bcastHeartbeatWithCtx(lastCtx.toByteArray())
        }
    }

    fun bcastHeartbeatWithCtx(ctx: ByteArray) {
        forEachProgress(BiConsumer { id, progress ->
            run {
                if (id == this.id) {
                    return@run
                }
                sendHeartbeat(id, ctx)
            }
        })
    }


    fun delProgress(id: Long) {
        this.prs.remove(id)
        this.learnerPrs.remove(id)
    }

    /** sendHeartbeat sends an empty MsgApp */
    fun sendHeartbeat(to: Long, ctx: ByteArray) {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        getProgress(to)?.run {
            val commit = Math.min(this.match, this@Raft.raftLog.committed)
            val m = Message.newBuilder()
                    .setTo(to)
                    .setType(MessageType.MsgHeartbeat)
                    .setCommit(commit)
                    .setContext(ByteString.copyFrom(ctx))
            send(m)
        } ?: log.warn {
            "Node[$to] didn't exist in prs : ${prs.keys}."
        }
    }

    private fun forEachProgress(biConsumer: BiConsumer<Long, Progress>) {
        this.prs.forEach(biConsumer)
        this.learnerPrs.forEach(biConsumer)
    }

    /**
     * pastElectionTimeout returns true if r.electionElapsed is greater than or equal to the randomized election timeout
     * in [electiontimeout, 2 * electiontimeout - 1]
     */
    fun pastElectionTimeout(): Boolean {
        return this.electionElapsed >= randomizedElectionTimeout
    }

    fun resetRandomizedElectionTimeout() {
        this.randomizedElectionTimeout = this.electionTimeout + RandomUtils.nextLong(0, this.electionTimeout)
    }


    /**
     * checkQuorumActive returns true if the quorum is active from
     * the view of the local raft state machine. Otherwise, it returns
     * false.checkQuorumActive also resets all RecentActive to false.
     */
    private fun checkQuorumActive(): Boolean {
        var active = 0

        forEachProgress(BiConsumer { id, progress ->
            run {
                if (id == this.id) { // self is always active
                    active++
                    return@run
                }
                if (progress.isRecentActive && !progress.isLearner) {
                    active++
                }
                progress.isRecentActive = false
            }
        })
        return active >= quorum()
    }

    private fun sendTimeoutNow(to: Long) = send(Message.newBuilder().setTo(to).setType(MessageType.MsgTimeoutNow))

    private fun abortLeaderTransfer() {
        this.leadTransferee = 0
    }

    fun numOfPendingConf(ents: List<Entry>?): Int {
        var n = 0
        if (ents != null) {
            for (entry in ents) {
                if (entry.type == EntryType.EntryConfChange) {
                    n++
                }
            }
        }
        return n
    }

    enum class StepType {
        StepFollower, StepCandidate, StepLeader
    }

    enum class TickType {
        Election, Heartbeat
    }

}
