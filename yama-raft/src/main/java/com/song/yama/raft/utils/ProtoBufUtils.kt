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

package com.song.yama.raft.utils

import com.google.protobuf.ByteString
import com.song.yama.raft.protobuf.RaftProtoBuf

object ProtoBufUtils {

    @JvmStatic
    fun buildEntry(index: Long) = RaftProtoBuf.Entry.newBuilder().setIndex(index).build()

    @JvmStatic
    fun buildEntry(data: ByteArray) = RaftProtoBuf.Entry.newBuilder().setData(ByteString.copyFrom(data)).build()

    @JvmStatic
    fun buildEntry(index: Long, term: Long) = RaftProtoBuf.Entry.newBuilder().setIndex(index).setTerm(term).build()

    @JvmStatic
    fun buildEntry(index: Long, term: Long, data: ByteArray) = RaftProtoBuf.Entry.newBuilder().setIndex(index).setTerm(term).setData(ByteString.copyFrom(data)).build()

    @JvmStatic
    fun buildMessage(from: Long, type: RaftProtoBuf.MessageType) = RaftProtoBuf.Message.newBuilder().setFrom(from).setType(type).build()

    @JvmStatic
    fun buildMessage(from: Long, type: RaftProtoBuf.MessageType, ctx: ByteArray) = RaftProtoBuf.Message.newBuilder().setFrom(from).setType(type).setContext(ByteString.copyFrom(ctx)).build()

    @JvmStatic
    fun buildMessage(from: Long, to: Long, type: RaftProtoBuf.MessageType) = RaftProtoBuf.Message.newBuilder().setFrom(from).setTo(to).setType(type).build()

    @JvmStatic
    fun buildMessage(from: Long, to: Long, type: RaftProtoBuf.MessageType, index: Long) = RaftProtoBuf.Message.newBuilder().setFrom(from).setTo(to).setType(type).setIndex(index).build()

    @JvmStatic
    fun buildMessage(from: Long, type: RaftProtoBuf.MessageType, index: Long) = RaftProtoBuf.Message.newBuilder().setFrom(from).setType(type).setIndex(index).build()

    @JvmStatic
    fun buildMessage(from: Long, type: RaftProtoBuf.MessageType, entries: MutableList<RaftProtoBuf.Entry>) = RaftProtoBuf.Message.newBuilder().setFrom(from).setType(type).addAllEntries(entries).build()

    @JvmStatic
    fun buildMessage(from: Long, to: Long, type: RaftProtoBuf.MessageType, entries: MutableList<RaftProtoBuf.Entry>) = RaftProtoBuf.Message.newBuilder().setFrom(from).setTo(to).setType(type).addAllEntries(entries).build()

    @JvmStatic
    fun buildMessage(from: Long, to: Long, type: RaftProtoBuf.MessageType, term: Long, entries: MutableList<RaftProtoBuf.Entry>) = RaftProtoBuf.Message.newBuilder().setFrom(from).setTo(to).setType(type).setTerm(term).addAllEntries(entries).build()

}
