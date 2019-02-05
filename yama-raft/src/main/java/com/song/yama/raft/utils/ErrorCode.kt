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

package com.song.yama.raft.utils

enum class ErrorCode private constructor(val code: Int, val desc: String) {
    OK(0, "ok"),

    ErrCompacted(1001, "requested index is unavailable due to compaction"),

    ErrSnapOutOfDate(1002, "requested index is older than the existing snapshot"),

    ErrUnavailable(1003, "requested entry at index is unavailable"),

    ErrSnapshotTemporarilyUnavailable(1004, "snapshot is temporarily unavailable"),

    ErrStopped(1005, "raft: stopped"),
    // ErrProposalDropped is returned when the proposal is ignored by some cases,
    // so that the proposer can be notified and fail fast.
    ErrProposalDropped(1006, "raft proposal dropped"),

    ErrExist(1007, "file already exists"),
    ErrEndOfFile(1008, "end of file"),
    ErrReadFromBufferedChannel(1009, "IO error while reading from buffered channel"),
    ErrInvalidVersion(1010, "Invalid version"),
    ErrBrokenFile(1011, "file is broken"),
    ErrInvalidCrc(1012, "crc check failed"),
    ErrFlushChannel(1013, "flush channel failed"),

    UNKNOWN(9999, "unknown exception");


    companion object {

        @JvmOverloads
        @JvmStatic
        fun getByCode(code: Int, defaultValue: ErrorCode = ErrorCode.UNKNOWN): ErrorCode {
            for (errorCode in values()) {
                if (errorCode.code == code) {
                    return errorCode
                }
            }

            return defaultValue
        }
    }

}
