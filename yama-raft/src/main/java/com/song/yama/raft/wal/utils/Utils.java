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

package com.song.yama.raft.wal.utils;


import java.io.File;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

    public static String commitLogName(long seq, long index) {
        return String.format("%016x-%016x.wal", seq, index);
    }


    public static boolean commitLogExists(String dir) {
        try {
            File file = new File(dir);
            if (!file.exists()) {
                return false;
            }

            if (!file.isDirectory()) {
                return false;
            }

            String[] names = file.list((dir1, name) -> name.endsWith(".wal"));
            return names != null && names.length != 0;
        } catch (Exception e) {
            log.info("Failed to check commit log existence :" + dir, e);
            return false;
        }
    }

}
