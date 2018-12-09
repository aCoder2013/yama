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

package com.song.yama.server.storage;

import java.io.Closeable;
import java.io.IOException;

/**
 * A generic key-value local database.
 */
public interface KeyValueStorage extends Closeable {

    void put(byte[] key, byte[] value) throws IOException;

    byte[] get(byte[] key) throws IOException;

    void delete(byte[] key) throws IOException;

    /**
     * Commit all pending write to durable storage.
     */
    void sync() throws IOException;
}
