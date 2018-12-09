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

package com.song.yama.server.protocol.raft.utils;

public enum ErrorCode {
    OK(0, "ok"),

    ErrCompacted(1001, "requested index is unavailable due to compaction"),

    ErrSnapOutOfDate(1002, "requested index is older than the existing snapshot"),

    ErrUnavailable(1003, "requested entry at index is unavailable"),

    ErrSnapshotTemporarilyUnavailable(1004, "snapshot is temporarily unavailable"),

    ErrStopped(1005, "raft: stopped"),
    // ErrProposalDropped is returned when the proposal is ignored by some cases,
    // so that the proposer can be notified and fail fast.
    ErrProposalDropped(1006, "raft proposal dropped"),

    UNKOWN(9999, "unknown exception"),;

    private int code;

    private String desc;

    ErrorCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static ErrorCode getByCode(int code){
        return getByCode(code, ErrorCode.UNKOWN);
    }

    public static ErrorCode getByCode(int code,ErrorCode defaultValue) {
        for (ErrorCode errorCode : values()) {
            if (errorCode.getCode() == code) {
                return errorCode;
            }
        }

        return defaultValue;
    }


}
