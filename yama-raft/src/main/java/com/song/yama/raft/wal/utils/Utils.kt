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

package com.song.yama.raft.wal.utils


import lombok.extern.slf4j.Slf4j
import org.slf4j.LoggerFactory
import java.io.File

@Slf4j
object Utils {

    private val log = LoggerFactory.getLogger(Utils::class.java)

    fun commitLogName(seq: Long, index: Long): String {
        return String.format("%016x-%016x.wal", seq, index)
    }

    fun commitLogExists(dir: String): Boolean {
        try {
            val file = File(dir)
            if (!file.exists()) {
                return false
            }

            if (!file.isDirectory) {
                return false
            }

            val names = file.list { dir1, name -> name.endsWith(".wal") }
            return names != null && names.isNotEmpty()
        } catch (e: Exception) {
            log.info("Failed to check commit log existence :$dir", e)
            return false
        }
    }

}
