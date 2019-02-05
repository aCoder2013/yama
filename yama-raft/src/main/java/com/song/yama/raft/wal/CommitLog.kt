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

package com.song.yama.raft.wal

import com.song.yama.common.utils.Result
import com.song.yama.raft.protobuf.RaftProtoBuf
import com.song.yama.raft.protobuf.RaftProtoBuf.Entry
import com.song.yama.raft.protobuf.RaftProtoBuf.HardState
import com.song.yama.raft.protobuf.WALRecord
import java.io.Closeable

/**
 * a storage interface for write ahead log WAL is a logical representation of the stable storage. WAL is either in read
 * mode or append mode but not both. A newly created WAL is in append mode, and ready for appending records. A just
 * opened WAL is in read mode, and ready for reading records. The WAL will be ready for appending after reading out all
 * the previous records.
 */
interface CommitLog : Closeable {

    fun save(hardState: HardState, ents: List<Entry>): Result<Void>

    fun saveSnap(snapshot: WALRecord.Snapshot): Result<Void>

    fun readAll(snapshot: RaftProtoBuf.Snapshot): Result<RaftStateRecord>
}
