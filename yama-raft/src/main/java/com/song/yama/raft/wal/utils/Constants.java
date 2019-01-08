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

public interface Constants {

    int OS_PAGE_SIZE = 1024 * 4;

    int COMMIT_LOG_FILE_SIZE = 512 * 1024 * 1024; // 512MB

    /**
     * File at the end of the minimum fixed length empty
     */
    int END_FILE_MIN_BLANK_LENGTH = 8;

    int RECORD_MAGIC_CODE = 0xecbde;

    int DEFAULT_WRITE_BUFFER_SIZE = 65536; //64KB
}
