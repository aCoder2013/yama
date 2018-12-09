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

package com.song.yama.server.storage.impl;

import static org.junit.Assert.assertEquals;

import com.song.yama.server.storage.KeyValueStorage;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RocksDBKeyValueStorageTest {

    private KeyValueStorage keyValueStorage;

    @Before
    public void setUp() throws Exception {
        keyValueStorage = new RocksDBKeyValueStorage("/home/admin/rocksdb/test", false);
    }

    @Test
    public void put() throws IOException {
        this.keyValueStorage.put("hello".getBytes(), "world".getBytes());
        assertEquals("world", new String(this.keyValueStorage.get("hello".getBytes())));
    }

    @Test
    public void delete() throws IOException {
        this.keyValueStorage.put("hello".getBytes(), "world".getBytes());
        assertEquals("world", new String(this.keyValueStorage.get("hello".getBytes())));
        this.keyValueStorage.delete("hello".getBytes());
        assertEquals(null, this.keyValueStorage.get("hello".getBytes()));
    }

    @After
    public void tearDown() throws Exception {
        keyValueStorage.close();
    }
}