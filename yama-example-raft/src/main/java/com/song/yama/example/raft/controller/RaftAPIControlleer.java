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

package com.song.yama.example.raft.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import com.song.yama.common.utils.Result;
import com.song.yama.example.raft.controller.request.ByteArrayBody;
import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping(value = "/yama/raft/api/v1")
public class RaftAPIControlleer {

    @Autowired
    private RaftNode raftNode;

    @PostMapping(value = "/send")
    public Result<Void> send(@RequestBody ByteArrayBody byteArrayBody) {
        byte[] body = byteArrayBody.getData();
        if (body == null || body.length == 0) {
            return Result.fail("Request body can't be null");
        }
        try {
            Message message = Message.newBuilder().mergeFrom(body).build();
            this.raftNode.processMessage(message);
            return Result.success();
        } catch (InvalidProtocolBufferException e) {
            log.error("Decode request failed", e);
            return Result.fail("Decode message body failed:" + e.getMessage());
        }
    }

}
