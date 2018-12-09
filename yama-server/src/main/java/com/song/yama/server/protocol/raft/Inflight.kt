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
package com.song.yama.server.protocol.raft

import com.song.yama.server.protocol.raft.exception.RaftException
import java.util.*

class Inflight {

    // the starting index in the buffer
    var start: Int = 0

    // number of inflights in the buffer
    var count: Int = 0

    var size: Int

    /**
     * buffer contains the index of the last entry inside one message.
     */
    var buffer: MutableList<Long>

    constructor() {
        this.size = DEFAULT_SIZE
        this.buffer = ArrayList(Collections.nCopies(DEFAULT_SIZE, 0L))
    }

    constructor(size: Int) {
        this.size = size
        this.buffer = ArrayList(Collections.nCopies(size, 0L))
    }

    constructor(start: Int, count: Int, size: Int, buffer: List<Long>) {
        this.start = start
        this.count = count
        this.size = size
        this.buffer = ArrayList(buffer)
    }


    fun reset() {
        this.count = 0
        this.start = 0
    }

    /**
     * add adds an inflight into inflights
     */
    fun add(inflight: Long) {
        if (isFull()) {
            throw RaftException("cannot add into a full inflights")
        }
        var next = this.start + this.count
        val size = this.size
        if (next >= size) {
            next -= size
        }

        this.buffer[next] = inflight
        this.count++
    }

    /**
     * only for test
     */
    fun addAll(vararg inflights: Long) {
        for (inflight in inflights) {
            add(inflight)
        }
    }

    fun freeFirstOne() {
        freeTo(this.buffer[this.start])
    }

    // freeTo frees the inflights smaller or equal to the given `to` flight.
    fun freeTo(to: Long) {
        if (this.count == 0 || to < this.buffer[this.start]) {
            return
        }
        var idx = this.start
        var index: Int = 0
        for (i in 0..this.count) {
            index = i
            if (to < this.buffer[idx]) { // found the first large inflight
                break
            }

            // increase index and maybe rotate
            val size = this.size
            idx++
            if (idx >= size) {
                idx -= size
            }
        }

        // free i inflights and set new start index
        this.count -= index
        this.start = idx
        if (this.count == 0) {
            // inflights is empty, reset the start index so that we don't grow the
            // buffer unnecessarily.
            this.start = 0
        }
    }

    fun isFull() = this.count == this.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Inflight

        if (size != other.size) return false
        if (buffer != other.buffer) return false

        return true
    }

    override fun hashCode(): Int {
        var result = size
        result = 31 * result + buffer.hashCode()
        return result
    }

    override fun toString(): String {
        return "Inflight(size=$size, buffer=$buffer)"
    }


    companion object {
        const val DEFAULT_SIZE = 16
    }
}
