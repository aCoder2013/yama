
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

package com.song.yama.common.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * a thread safe id generator based {@link AtomicInteger}, if the id reached {@link Integer#MAX_VALUE}, then it'll be
 * reset to zero
 */
public class AtomicIdGenerator implements IdGenerator {

    private final AtomicInteger idGenerator = new AtomicInteger();

    @Override
    public int generateId() {
        int i = idGenerator.incrementAndGet();
        if (i == Integer.MAX_VALUE) {
            synchronized (this) {
                if (idGenerator.get() == Integer.MAX_VALUE) {
                    idGenerator.set(0);
                    i = idGenerator.incrementAndGet();
                }
            }
        }
        return i;
    }
}
