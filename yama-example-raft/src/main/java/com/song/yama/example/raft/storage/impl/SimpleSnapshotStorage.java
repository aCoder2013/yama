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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.io.Files;
import com.song.yama.example.raft.exception.ErrNoSnapshotException;
import com.song.yama.example.raft.storage.SnapshotStorage;
import com.song.yama.raft.exception.RaftException;
import com.song.yama.raft.protobuf.RaftProtoBuf.Snapshot;
import com.song.yama.raft.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple Snapshot storage ,only for test
 */
@Slf4j
public class SimpleSnapshotStorage implements SnapshotStorage {

    private final String directory;

    public SimpleSnapshotStorage(String directory) {
        checkArgument(directory != null && !directory.equals(""));
        this.directory = directory;
    }

    @Override
    public void save(Snapshot snapshot) throws IOException {
        if (Utils.isEmptySnap(snapshot)) {
            return;
        }

        String fileName = directory
            .concat("snapshot_" + snapshot.getMetadata().getTerm() + "_" + snapshot.getMetadata().getIndex() + ".snap");
        Files.write(snapshot.toByteArray(), new File(fileName));
    }

    @Override
    public Snapshot load() {
        List<String> names = names();
        for (String s : names) {
            String path = directory.concat(s);
            try {
                byte[] data = Files.toByteArray(new File(path));
                if (data != null && data.length > 0) {
                    return Snapshot.newBuilder().mergeFrom(data).build();
                }
            } catch (IOException e) {
                log.warn("Failed to read snap file:" + path, e);
                throw new RaftException("Failed to read snap", e);
            }
        }
        return null;
    }

    @Override
    public Snapshot read(String name) throws IOException {
        byte[] data = Files.toByteArray(new File(name));
        if (data != null && data.length > 0) {
            return Snapshot.newBuilder().mergeFrom(data).build();
        }
        throw new ErrNoSnapshotException();
    }

    @Override
    public List<String> names() {
        List<String> names = new ArrayList<>();
        File file = new File(directory);
        File[] files = file.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && f.getName().endsWith(".snap")) {
                    names.add(f.getName());
                }
            }
        }
        return names;
    }

}
