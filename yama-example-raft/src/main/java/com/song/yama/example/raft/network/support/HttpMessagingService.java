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

package com.song.yama.example.raft.network.support;

import com.alibaba.fastjson.JSONObject;
import com.song.yama.example.raft.controller.request.ByteArrayBody;
import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.example.raft.network.MessagingService;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HttpMessagingService implements MessagingService {

    public static final MediaType JSON
        = MediaType.parse("application/json; charset=utf-8");

    private AtomicReference<List<String>> hosts = new AtomicReference<>();

    @Autowired
    private RaftNode raftNode;

    private OkHttpClient okHttpClient = new OkHttpClient.Builder()
        .readTimeout(6, TimeUnit.SECONDS)
        .connectTimeout(5, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .build();

    @Override
    @PostConstruct
    public void start() {
        log.info("Start http based messaging service");
        this.hosts.set(raftNode.getPeers());
    }

    @Override
    public synchronized void send(List<Message> messages) {
        if (CollectionUtils.isEmpty(this.hosts.get())) {
            this.hosts.set(raftNode.getPeers());
        }
        if (CollectionUtils.isEmpty(this.hosts.get())) {
            return;
        }
        List<String> hosts = this.hosts.get();
        if (CollectionUtils.isNotEmpty(messages)) {
            messages.forEach(message -> {
                if (message == null) {
                    return;
                }
                if (message.getTo() == 0L) {
                    return;
                }
                String host = hosts.get((int) (message.getTo() - 1));
                RequestBody body = RequestBody
                    .create(JSON, JSONObject.toJSONString(new ByteArrayBody(message.toByteArray())));
                Request request = new Request.Builder()
                    .url(String.format("http://%s/yama/raft/api/v1/send", host))
                    .post(body)
                    .build();
                try {
                    try (Response response = okHttpClient.newCall(request).execute()) {
                        log.info("Send message to host[{}] type:{}.", host, message.getType());
                    }
                } catch (IOException e) {
                    log.warn("Send message to remote failed,you may ignore this warning if the system is starting up."
                        + host, e);
                }
            });
        }
    }

    @Override
    public void refreshHosts(List<String> hosts) {
        for (; ; ) {
            List<String> oldHosts = this.hosts.get();
            if (this.hosts.compareAndSet(oldHosts, hosts)) {
                return;
            }
        }
    }

    @Override
    public void close() throws Exception {
    }
}
