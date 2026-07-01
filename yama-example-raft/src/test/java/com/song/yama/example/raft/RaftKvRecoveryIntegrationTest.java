package com.song.yama.example.raft;

import static org.junit.Assert.assertEquals;

import com.song.yama.example.raft.core.RaftNode;
import com.song.yama.raft.StateType;
import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import com.song.yama.example.raft.ExampleRaftApplication;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.RestTemplate;

/**
 * End-to-end restart tests: start app, write KV, stop, start again, verify data.
 */
public class RaftKvRecoveryIntegrationTest {

    private String dataDir;
    private ConfigurableApplicationContext context;

    @After
    public void cleanup() {
        if (context != null) {
            context.close();
        }
        if (dataDir != null) {
            deleteRecursively(new File(dataDir));
        }
    }

    @Test
    public void walOnlyRestartPreservesKv() throws Exception {
        dataDir = Files.createTempDirectory("yama-kv-wal-" + UUID.randomUUID()).toString();

        context = startApp(dataDir);
        waitForLeader(context);
        int port = serverPort(context);
        RestTemplate client = new RestTemplate();

        put(client, port, "name", "alice");
        put(client, port, "city", "beijing");
        assertEquals("alice", get(client, port, "name"));

        context.close();
        context = null;
        Thread.sleep(300);

        context = startApp(dataDir);
        waitForLeader(context);
        port = serverPort(context);
        client = new RestTemplate();

        assertEquals("alice", get(client, port, "name"));
        assertEquals("beijing", get(client, port, "city"));
    }

    @Test
    public void snapshotRestartPreservesKv() throws Exception {
        dataDir = Files.createTempDirectory("yama-kv-snap-" + UUID.randomUUID()).toString();

        context = startApp(dataDir);
        waitForLeader(context);
        int port = serverPort(context);
        RestTemplate client = new RestTemplate();

        for (int i = 0; i < 12; i++) {
            put(client, port, "k" + i, "v" + i);
        }
        Thread.sleep(1500);

        assertEquals("v0", get(client, port, "k0"));
        assertEquals("v11", get(client, port, "k11"));

        context.close();
        context = null;
        Thread.sleep(300);

        context = startApp(dataDir);
        waitForLeader(context);
        port = serverPort(context);
        client = new RestTemplate();

        assertEquals("v0", get(client, port, "k0"));
        assertEquals("v11", get(client, port, "k11"));
    }

    private static ConfigurableApplicationContext startApp(String dataDir) {
        SpringApplication app = new SpringApplication(ExampleRaftApplication.class);
        Map<String, Object> props = new HashMap<>();
        props.put("server.port", "0");
        props.put("com.song.yama.raft.id", "1");
        props.put("com.song.yama.raft.servers", "127.0.0.1:9001");
        props.put("com.song.yama.raft.join", "false");
        props.put("com.song.yama.raft.data-dir", dataDir);
        return app.run(toArgs(props));
    }

    private static String[] toArgs(Map<String, Object> props) {
        return props.entrySet().stream()
            .map(e -> "--" + e.getKey() + "=" + e.getValue())
            .toArray(String[]::new);
    }

    private static void waitForLeader(ConfigurableApplicationContext context) throws InterruptedException {
        RaftNode raftNode = context.getBean(RaftNode.class);
        for (int i = 0; i < 50; i++) {
            if (raftNode.getNode().status().getSoftState().getRaftState() == StateType.LEADER) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("leader not elected within timeout");
    }

    private static int serverPort(ConfigurableApplicationContext context) {
        ServletWebServerApplicationContext web = (ServletWebServerApplicationContext) context;
        return web.getWebServer().getPort();
    }

    private static void put(RestTemplate client, int port, String key, String value) throws InterruptedException {
        String body = client.getForObject(
            "http://127.0.0.1:" + port + "/yama/raft/api/v1/put?key=" + key + "&value=" + value,
            String.class);
        assertEquals("ok", body);
        waitFor(client, port, key, value);
    }

    private static void waitFor(RestTemplate client, int port, String key, String expected)
        throws InterruptedException {
        for (int i = 0; i < 50; i++) {
            String actual = get(client, port, key);
            if (expected.equals(actual)) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(expected, get(client, port, key));
    }

    private static String get(RestTemplate client, int port, String key) {
        return client.getForObject(
            "http://127.0.0.1:" + port + "/yama/raft/api/v1/get?key=" + key,
            String.class);
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
}
