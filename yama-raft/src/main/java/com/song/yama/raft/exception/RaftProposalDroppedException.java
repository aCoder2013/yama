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

package com.song.yama.raft.exception;

/**
 * ErrProposalDropped is returned when the proposal is ignored by some cases, so that the proposer can be notified and
 * fail fast.
 */
public class RaftProposalDroppedException extends RuntimeException {

    public RaftProposalDroppedException() {
        super("raft proposal dropped");
    }
}
