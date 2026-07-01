package com.song.yama.example.raft.support;

import com.song.yama.example.raft.ExampleRaftApplication;
import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.raft.StateType;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Manages a multi-node Spring Boot Raft KV cluster for integration tests.
 */
public final class RaftKvClusterHarness implements AutoCloseable {

    private final String dataDir;
    private final List<NodeHandle> nodes = new ArrayList<>();
    private final String servers;
    private final int[] ports;

    private RaftKvClusterHarness(String dataDir, int[] ports, String servers) {
        this.dataDir = dataDir;
        this.ports = ports.clone();
        this.servers = servers;
    }

    public static RaftKvClusterHarness startThreeNodeCluster() throws Exception {
        FaultInjectionRegistry.recover();
        String dataDir = Files.createTempDirectory("yama-kv-it-" + UUID.randomUUID()).toString();
        int[] ports = allocatePorts(3);
        String servers = buildServers(ports);
        RaftKvClusterHarness harness = new RaftKvClusterHarness(dataDir, ports, servers);
        for (int id = 1; id <= 3; id++) {
            harness.bootNode(id);
        }
        harness.waitForLeader(80);
        Thread.sleep(1500);
        return harness;
    }

    public void putOnLeader(RaftKvHttpClient client, String key, String value) throws InterruptedException {
        waitForLeader(80);
        client.put(leaderPort(), key, value);
        assertKvConsistent(client, key, value);
    }

    public NodeHandle node(int id) {
        return nodes.get(id - 1);
    }

    public int leaderId() {
        for (NodeHandle handle : nodes) {
            if (handle.isRunning() && handle.isLeader()) {
                return handle.getId();
            }
        }
        return -1;
    }

    public int leaderPort() {
        int id = leaderId();
        if (id < 0) {
            return -1;
        }
        return node(id).getPort();
    }

    public void waitForLeader(int attempts) throws InterruptedException {
        for (int i = 0; i < attempts; i++) {
            if (leaderId() > 0) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("leader not elected within timeout");
    }

    public void stopNode(int id) throws InterruptedException {
        NodeHandle handle = node(id);
        if (handle.context != null) {
            handle.context.close();
            handle.context = null;
            Thread.sleep(800);
        }
    }

    public void restartNode(int id) throws InterruptedException {
        stopNode(id);
        bootNode(id);
        waitForLeader(60);
        Thread.sleep(500);
    }

    public void assertKvConsistent(RaftKvHttpClient client, String key, String expected) throws InterruptedException {
        for (NodeHandle handle : nodes) {
            if (!handle.isRunning()) {
                continue;
            }
            client.waitFor(handle.getPort(), key, expected, 100);
        }
    }

    public int countLeadersExcluding(int excludedId) {
        int count = 0;
        for (NodeHandle handle : nodes) {
            if (handle.isRunning() && handle.getId() != excludedId && handle.isLeader()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        for (NodeHandle handle : nodes) {
            if (handle.context != null) {
                handle.context.close();
            }
        }
        deleteRecursively(new File(dataDir));
        FaultInjectionRegistry.recover();
    }

    private void bootNode(int id) {
        NodeHandle handle = new NodeHandle(id, ports[id - 1]);
        if (nodes.size() >= id) {
            nodes.set(id - 1, handle);
        } else {
            while (nodes.size() < id - 1) {
                nodes.add(null);
            }
            nodes.add(handle);
        }

        SpringApplication app = new SpringApplication(ExampleRaftApplication.class, FaultTestConfiguration.class);
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", String.valueOf(ports[id - 1]));
        props.put("com.song.yama.raft.id", String.valueOf(id));
        props.put("com.song.yama.raft.servers", servers);
        props.put("com.song.yama.raft.join", "false");
        props.put("com.song.yama.raft.data-dir", dataDir + "/node-" + id);
        props.put("spring.profiles.active", "integration-fault-test");
        handle.context = app.run(toArgs(props));
        handle.port = serverPort(handle.context);
    }

    private static int[] allocatePorts(int count) throws IOException {
        int[] ports = new int[count];
        ServerSocket[] sockets = new ServerSocket[count];
        try {
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
            return ports;
        } finally {
            for (ServerSocket socket : sockets) {
                if (socket != null) {
                    socket.close();
                }
            }
        }
    }

    private static String buildServers(int[] ports) {
        return IntStream.of(ports)
            .mapToObj(port -> "127.0.0.1:" + port)
            .collect(Collectors.joining(";"));
    }

    private static String[] toArgs(Map<String, Object> props) {
        return props.entrySet().stream()
            .map(e -> "--" + e.getKey() + "=" + e.getValue())
            .toArray(String[]::new);
    }

    private static int serverPort(ConfigurableApplicationContext context) {
        return ((ServletWebServerApplicationContext) context).getWebServer().getPort();
    }

    private static void deleteRecursively(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }

    public static final class NodeHandle {
        private final int id;
        private int port;
        private ConfigurableApplicationContext context;

        private NodeHandle(int id, int port) {
            this.id = id;
            this.port = port;
        }

        public int getId() {
            return id;
        }

        public int getPort() {
            return port;
        }

        public boolean isRunning() {
            return context != null && context.isActive();
        }

        public boolean isLeader() {
            if (!isRunning()) {
                return false;
            }
            RaftNode raftNode = context.getBean(RaftNode.class);
            return raftNode.getNode().status().getSoftState().getRaftState() == StateType.LEADER;
        }
    }
}
