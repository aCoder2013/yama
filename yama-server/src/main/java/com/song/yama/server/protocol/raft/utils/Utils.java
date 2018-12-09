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

package com.song.yama.server.protocol.raft.utils;

import com.song.yama.server.protocol.raft.RaftLog;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.Entry;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.HardState;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.MessageType;
import com.song.yama.server.protocol.raft.protobuf.RaftProtoBuf.Snapshot;
import java.util.Collections;
import java.util.List;
import org.springframework.util.CollectionUtils;

public class Utils {

    public static final long INVALID_ID = 0;

    public static final Long NO_LIMIT = Long.MAX_VALUE;

    public static final HardState EMPTY_HARD_STATE = HardState.newBuilder().build();

    public static boolean isHardStateEqual(HardState a, HardState b) {
        return a.getTerm() == b.getTerm() && a.getVote() == b.getVote() && a.getCommit() == b
            .getCommit();
    }

    /**
     * IsEmptyHardState returns true if the given HardState is empty
     */
    public static boolean isEmptyHardState(HardState st) {
        return isHardStateEqual(st, Utils.EMPTY_HARD_STATE);
    }

    public static boolean isEmptySnap(Snapshot snapshot) {
        return snapshot.getMetadata().getIndex() == 0;
    }

    public static List<Entry> limitSize(List<Entry> ents, long maxSize) {
        if (CollectionUtils.isEmpty(ents)) {
            return Collections.emptyList();
        }

        long size = ents.get(0).getSerializedSize();
        int limit;
        for (limit = 1; limit < ents.size(); limit++) {
            size += ents.get(limit).getSerializedSize();
            if (size > maxSize) {
                break;
            }
        }
        return ents.subList(0, limit);
    }

    /**
     * voteResponseType maps vote and prevote message types to their corresponding responses.
     */
    public static MessageType voteRespMsgType(MessageType msgt) {
        if (msgt == MessageType.MsgVote) {
            return MessageType.MsgVoteResp;
        } else if (msgt == MessageType.MsgPreVote) {
            return MessageType.MsgPreVoteResp;
        } else {
            throw new IllegalArgumentException("Not a vote message :" + msgt);
        }
    }

    public static boolean isLocalMessage(MessageType messageType) {
        return messageType == MessageType.MsgHup || messageType == MessageType.MsgBeat
            || messageType == MessageType.MsgUnreachable
            || messageType == MessageType.MsgSnapStatus
            || messageType == MessageType.MsgCheckQuorum;
    }

    public static boolean isResponseMessage(MessageType msgt) {
        return msgt == MessageType.MsgAppResp
            || msgt == MessageType.MsgVoteResp
            || msgt == MessageType.MsgHeartbeatResp
            || msgt == MessageType.MsgUnreachable
            || msgt == MessageType.MsgPreVoteResp;
    }


    public static String ltoa(RaftLog l) {
        StringBuilder sb = new StringBuilder(128);
        sb.append(String.format("committed:%d\n", l.getCommitted()));
        sb.append(String.format("applied:  %d\n", l.getApplied()));
        List<Entry> entries = l.allEntries();
        for (int i = 0; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            sb.append("#").append(i).append(": ").append(entry).append("\n");
        }
        return sb.toString();
    }
}
