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

package com.song.yama.example.raft.config;

import com.song.yama.example.raft.properties.RaftProperties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class RaftConfiguration {

    @Autowired
    private RaftProperties raftProperties;

    @PostConstruct
    public void init() {
        log.info("Raft servers :{}.", raftProperties.getServers());
    }

    @Bean
    public BlockingQueue<String> proposeQueue(){
        return new LinkedBlockingQueue<>();
    }

    @Bean
    public BlockingQueue<String> commitQueue(){
        return new LinkedBlockingDeque<>();
    }
}
