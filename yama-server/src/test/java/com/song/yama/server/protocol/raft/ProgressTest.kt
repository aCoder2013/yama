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

package com.song.yama.server.protocol.raft

import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class ProgressTest {

    @Test
    fun testInflightsAdd() {
        val inflight = Inflight(10)
        for (i in 0..4) {
            inflight.add(i.toLong())
        }

        val wantIn = Inflight(0, 5, 10, mutableListOf(0, 1, 2, 3, 4, 0, 0, 0, 0, 0))
        assertEquals(inflight, wantIn)

        for (i in 5..9) {
            inflight.add(i.toLong())
        }

        val wantIn2 = Inflight(0, 10, 10, mutableListOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        assertEquals(inflight, wantIn2)

        val inflight2 = Inflight(5, 0, 10, ArrayList<Long>(Collections.nCopies(10, 0L)))

        for (i in 0..4) {
            inflight2.add(i.toLong())
        }

        val wantIn21 = Inflight(5, 5, 10, mutableListOf(0, 0, 0, 0, 0, 0, 1, 2, 3, 4))
        assertEquals(inflight2, wantIn21)

        for (i in 5..9) {
            inflight2.add(i.toLong())
        }

        val wantIn22 = Inflight(5, 10, 10, mutableListOf(5, 6, 7, 8, 9, 0, 1, 2, 3, 4))
        assertEquals(inflight2, wantIn22)
    }


    @Test
    fun testInflightFreeTo() {
        // no rotating case
        val inflight = Inflight(10)
        for (i in 0..9) {
            inflight.add(i.toLong())
        }

        inflight.freeTo(4)

        val wantIn = Inflight(5, 5, 10, mutableListOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        assertEquals(inflight, wantIn)

        inflight.freeTo(4)
        val wantIn2 = Inflight(9, 1, 10, mutableListOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        assertEquals(inflight, wantIn2)

        // rotating case
        for (i in 10..14) {
            inflight.add(i.toLong())
        }

        inflight.freeTo(12)

        val wantIn3 = Inflight(3, 2, 10, mutableListOf(10, 11, 12, 13, 14, 5, 6, 7, 8, 9))

        assertEquals(inflight, wantIn3)

        inflight.freeTo(14)

        val wantIn4 = Inflight(0, 0, 10, mutableListOf(10, 11, 12, 13, 14, 5, 6, 7, 8, 9))
        assertEquals(inflight, wantIn4)
    }

    @Test
    fun testInflightFreeFirstOne() {
        val inflight = Inflight(10)
        for (i in 0..9) {
            inflight.add(i.toLong())
        }

        inflight.freeFirstOne()

        val wantIn = Inflight(1, 9, 10, mutableListOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
        assertEquals(inflight, wantIn)

    }
}