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

import com.song.yama.example.raft.network.MessagingService;
import com.song.yama.raft.Peer;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HttpMessagingService implements MessagingService {

    private List<String> hosts;

    private OkHttpClient okHttpClient = new OkHttpClient.Builder()
        .readTimeout(6, TimeUnit.SECONDS)
        .connectTimeout(5, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .build();
    private List<Peer> peers;
//
//    public HttpMessagingService(List<String> hosts, List<Peer> peers) {
//        checkArgument(CollectionUtils.isNotEmpty(hosts));
//        this.hosts = new ArrayList<>(hosts);
//        checkArgument(CollectionUtils.isNotEmpty(peers));
//        this.peers = peers;
//    }

    @Override
    public void start() {
        log.info("Start http based messaging service");
    }

    @Override
    public void send(List<Message> messages) {
        if (CollectionUtils.isNotEmpty(messages)) {
            messages.forEach(message -> {
                if (message.getTo() == 0L) {
                    return;
                }
                //TODO: send message to remote peer
            });
        }
    }

    @Override
    public void close() throws Exception {

    }
}
