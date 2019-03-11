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

package com.song.yama.example.raft.core;

import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;

public interface StateMachine {

    void propose(String key, String value);

    String lookup(String key);

    void processCommits(String commit);

    void loadSnapshot();

    void loadSnapshot(Snapshot snapshot);

    byte[] getSnapshot();
}
