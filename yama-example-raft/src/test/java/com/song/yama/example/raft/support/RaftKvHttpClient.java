package com.song.yama.example.raft.support;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public final class RaftKvHttpClient {

    private final RestTemplate restTemplate = new RestTemplate();

    public void put(int port, String key, String value) throws InterruptedException {
        String body = restTemplate.getForObject(putUri(port, key, value), String.class);
        assertEquals("ok", body);
        waitFor(port, key, value, 50);
    }

    public String get(int port, String key) {
        return restTemplate.getForObject(getUri(port, key), String.class);
    }

    public boolean isReadUnavailable(int port, String key) {
        try {
            get(port, key);
            return false;
        } catch (org.springframework.web.client.HttpServerErrorException e) {
            return e.getRawStatusCode() == 503;
        }
    }

    public void waitForReadUnavailable(int port, String key, int attempts) throws InterruptedException {
        for (int i = 0; i < attempts; i++) {
            if (isReadUnavailable(port, key)) {
                return;
            }
            Thread.sleep(100);
        }
        if (!isReadUnavailable(port, key)) {
            throw new AssertionError("Expected read to be unavailable on port " + port + " for key " + key);
        }
    }

    public void waitFor(int port, String key, String expected, int attempts) throws InterruptedException {
        for (int i = 0; i < attempts; i++) {
            String actual = get(port, key);
            if (expected == null ? actual == null : expected.equals(actual)) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(expected, get(port, key));
    }

    private static URI putUri(int port, String key, String value) {
        return UriComponentsBuilder
            .fromHttpUrl(baseUrl(port))
            .path("/put")
            .queryParam("key", key)
            .queryParam("value", value)
            .build()
            .encode()
            .toUri();
    }

    private static URI getUri(int port, String key) {
        return UriComponentsBuilder
            .fromHttpUrl(baseUrl(port))
            .path("/get")
            .queryParam("key", key)
            .build()
            .encode()
            .toUri();
    }

    private static String baseUrl(int port) {
        return "http://127.0.0.1:" + port + "/yama/raft/api/v1";
    }
}
