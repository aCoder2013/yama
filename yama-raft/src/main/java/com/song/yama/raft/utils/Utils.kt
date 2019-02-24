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

package com.song.yama.raft.utils

import com.song.yama.raft.RaftLog
import com.song.yama.raft.protobuf.RaftProtoBuf.*
import org.apache.commons.collections4.CollectionUtils

object Utils {

    const val INVALID_ID: Long = 0

    const val NO_LIMIT = java.lang.Long.MAX_VALUE

    val EMPTY_HARD_STATE: HardState = HardState.newBuilder().build()

    fun isHardStateEqual(a: HardState, b: HardState): Boolean {
        return a.term == b.term && a.vote == b.vote && a.commit == b
                .commit
    }

    /**
     * IsEmptyHardState returns true if the given HardState is empty
     */
    fun isEmptyHardState(st: HardState): Boolean {
        return isHardStateEqual(st, Utils.EMPTY_HARD_STATE)
    }

    fun isEmptySnap(snapshot: Snapshot?): Boolean {
        return snapshot == null || snapshot.metadata == null || snapshot.metadata.index == 0L
    }

    fun limitSize(ents: List<Entry>, maxSize: Long): List<Entry> {
        if (CollectionUtils.isEmpty(ents)) {
            return emptyList()
        }

        var size = ents[0].serializedSize.toLong()
        var limit = 1
        while (limit < ents.size) {
            size += ents[limit].serializedSize.toLong()
            if (size > maxSize) {
                break
            }
            limit++
        }
        return ents.subList(0, limit)
    }

    /**
     * voteResponseType maps vote and prevote message types to their corresponding responses.
     */
    fun voteRespMsgType(msgt: MessageType): MessageType {
        return if (msgt == MessageType.MsgVote) {
            MessageType.MsgVoteResp
        } else if (msgt == MessageType.MsgPreVote) {
            MessageType.MsgPreVoteResp
        } else {
            throw IllegalArgumentException("Not a vote message :$msgt")
        }
    }

    fun isLocalMessage(messageType: MessageType): Boolean {
        return (messageType == MessageType.MsgHup || messageType == MessageType.MsgBeat
                || messageType == MessageType.MsgUnreachable
                || messageType == MessageType.MsgSnapStatus
                || messageType == MessageType.MsgCheckQuorum)
    }

    fun isResponseMessage(msgt: MessageType): Boolean {
        return (msgt == MessageType.MsgAppResp
                || msgt == MessageType.MsgVoteResp
                || msgt == MessageType.MsgHeartbeatResp
                || msgt == MessageType.MsgUnreachable
                || msgt == MessageType.MsgPreVoteResp)
    }


    fun ltoa(l: RaftLog): String {
        val sb = StringBuilder(128)
        sb.append(String.format("committed:%d\n", l.committed))
        sb.append(String.format("applied:  %d\n", l.applied))
        val entries = l.allEntries()
        for (i in entries.indices) {
            val entry = entries[i]
            sb.append("#").append(i).append(": ").append(entry).append("\n")
        }
        return sb.toString()
    }
}
