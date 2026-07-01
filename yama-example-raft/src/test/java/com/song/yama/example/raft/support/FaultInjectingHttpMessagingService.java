package com.song.yama.example.raft.support;

import com.alibaba.fastjson.JSONObject;
import com.song.yama.example.raft.controller.request.ByteArrayBody;
import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.example.raft.network.MessagingService;
import com.song.yama.raft.protobuf.RaftProtoBuf.Message;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.collections4.CollectionUtils;

/**
 * HTTP messaging with optional network fault injection for integration tests.
 */
@Slf4j
public class FaultInjectingHttpMessagingService implements MessagingService {

    public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final RaftNode raftNode;
    private final AtomicReference<List<String>> hosts = new AtomicReference<>();
    private final OkHttpClient okHttpClient = new OkHttpClient.Builder()
        .readTimeout(6, TimeUnit.SECONDS)
        .connectTimeout(5, TimeUnit.SECONDS)
        .writeTimeout(10, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .build();

    public FaultInjectingHttpMessagingService(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void start() {
        this.hosts.set(raftNode.getPeers());
    }

    @Override
    public synchronized void send(List<Message> messages) {
        if (CollectionUtils.isEmpty(this.hosts.get())) {
            this.hosts.set(raftNode.getPeers());
        }
        List<String> peerHosts = this.hosts.get();
        if (CollectionUtils.isEmpty(peerHosts) || CollectionUtils.isEmpty(messages)) {
            return;
        }
        long localId = raftNode.getId();
        messages.forEach(message -> {
            if (message == null || message.getTo() == 0L) {
                return;
            }
            if (FaultInjectionRegistry.shouldDrop(localId, message.getTo())) {
                log.debug("Fault injection: drop message from {} to {}, type={}",
                    localId, message.getTo(), message.getType());
                return;
            }
            String host = peerHosts.get((int) (message.getTo() - 1));
            RequestBody body = RequestBody.create(JSON,
                JSONObject.toJSONString(new ByteArrayBody(message.toByteArray())));
            Request request = new Request.Builder()
                .url(String.format("http://%s/yama/raft/api/v1/send", host))
                .post(body)
                .build();
            try (Response response = okHttpClient.newCall(request).execute()) {
                log.debug("Send message to host[{}] type:{}.", host, message.getType());
            } catch (IOException e) {
                log.debug("Send message to remote failed: {}", host, e);
            }
        });
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
    public void close() {
    }
}
