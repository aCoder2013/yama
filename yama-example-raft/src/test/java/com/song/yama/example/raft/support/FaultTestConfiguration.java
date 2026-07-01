package com.song.yama.example.raft.support;

import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.example.raft.network.MessagingService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Profile("integration-fault-test")
@TestConfiguration
public class FaultTestConfiguration {

    @Bean
    @Primary
    public MessagingService faultInjectingMessagingService(RaftNode raftNode) {
        FaultInjectingHttpMessagingService service = new FaultInjectingHttpMessagingService(raftNode);
        service.start();
        return service;
    }
}
