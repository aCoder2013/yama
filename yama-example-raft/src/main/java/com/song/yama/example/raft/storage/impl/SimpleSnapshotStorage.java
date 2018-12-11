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

package com.song.yama.example.raft.storage.impl;

import com.song.yama.example.raft.storage.SnapshotStorage;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SimpleSnapshotStorage implements SnapshotStorage{

    @Override
    public void save(Snapshot snapshot) {

    }

    @Override
    public Snapshot load() {
        return null;
    }

    @Override
    public Snapshot read(String name) {
        return null;
    }

    @Override
    public List<String> names() {
        return null;
    }
}
