package com.song.yama.example.raft.support;

/**
 * Sample KV payloads that stress JSON encoding under Raft fault scenarios.
 */
public final class JsonKvTestValues {

    public static final String UNICODE_VALUE = "中文-emoji-🚀";
    public static final String QUOTED_VALUE = "say \"hello\" and 'world'";
    public static final String ESCAPED_VALUE = "line1\nline2\ttab\\backslash";
    public static final String JSON_LIKE_VALUE = "{\"nested\":\"value\",\"n\":42}";
    public static final String EMPTY_VALUE = "";

    private JsonKvTestValues() {
    }
}
