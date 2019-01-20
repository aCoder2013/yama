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

package com.song.yama.messaging.message;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.song.yama.common.utils.AtomicIdGenerator;
import com.song.yama.common.utils.IdGenerator;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

@Data
public class RemotingMessage {

    private static final IdGenerator idGenerator = new AtomicIdGenerator();

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final RemotingMessage remotingMessage;

        private Builder() {
            this.remotingMessage = new RemotingMessage();
        }

        public Builder withVersion(int version) {
            this.remotingMessage.setVersion(version);
            return this;
        }

        public Builder withSubject(String subject) {
            remotingMessage.setSubject(subject);
            return this;
        }

        public Builder withBody(byte[] body) {
            remotingMessage.setBody(body);
            return this;
        }

        public Builder withCode(int code) {
            remotingMessage.setCode(code);
            return this;
        }

        public Builder withMessage(String message) {
            remotingMessage.setMessage(message);
            return this;
        }

        public Builder withHeader(String key, String value) {
            if (remotingMessage.getHeaders() == null) {
                remotingMessage.setHeaders(new HashMap<>());
            }
            remotingMessage.getHeaders().put(key, value);
            return this;
        }

        public Builder withHeaders(Map<String, String> headers) {
            remotingMessage.setHeaders(headers);
            return this;
        }

        public RemotingMessage build() {
            return this.remotingMessage;
        }

    }

    private int version;

    private int requestId = idGenerator.generateId();

    private String subject;

    private int code;

    private String message;

    private Map<String, String> headers;

    private byte[] body;

    public ByteBuffer encode() {

        //version
        int length = 4;

        //requestId
        length += 4;

        //subject
        length += 4;
        length += subject.length();

        //code
        length += 4;

        //message
        if (StringUtils.isNotBlank(message)) {
            length += 4;
            length += message.getBytes().length;
        }

        //header
        byte[] headerBytes = new byte[0];
        if (headers != null && headers.size() > 0) {
            headerBytes = JSON.toJSONBytes(headers);
        }

        length += headerBytes.length;

        //body
        if (body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);
        result.putInt(length);
        result.putInt(version);
        result.putInt(requestId);
        result.putInt(subject.length());
        result.put(subject.getBytes());
        result.putInt(code);
        result.putInt(message.getBytes().length);
        result.put(message.getBytes());
        result.putInt(headerBytes.length);
        result.put(headerBytes);
        if (body != null) {
            result.put(body);
        }
        result.flip();
        return result;
    }

    public static RemotingMessage decode(ByteBuffer byteBuffer) {
        RemotingMessage remotingMessage = new RemotingMessage();
        int length = byteBuffer.limit();
        int version = byteBuffer.getInt();
        remotingMessage.setVersion(version);
        if (version == 0) {
            int requestId = byteBuffer.getInt();
            remotingMessage.setRequestId(requestId);
            int subjectLength = byteBuffer.getInt();
            if (subjectLength > 0) {
                byte[] subjectBytes = new byte[subjectLength];
                byteBuffer.get(subjectBytes);
                remotingMessage.setSubject(new String(subjectBytes));
            }

            remotingMessage.setCode(byteBuffer.getInt());

            int messageLength = byteBuffer.getInt();
            if (messageLength > 0) {
                byte[] messageBytes = new byte[messageLength];
                byteBuffer.get(messageBytes);
                remotingMessage.setMessage(new String(messageBytes));
            }

            int headerLength = byteBuffer.getInt();
            if (headerLength > 0) {
                byte[] headerBytes = new byte[headerLength];
                byteBuffer.get(headerBytes);
                Map<String, String> headers = JSON
                    .parseObject(new String(headerBytes), new TypeReference<Map<String, String>>() {
                    });
                remotingMessage.setHeaders(headers);
            }

            int bodyLength = length - 4 - subjectLength - headerLength;
            byte[] bodyBytes;
            if (bodyLength > 0) {
                bodyBytes = new byte[bodyLength];
                byteBuffer.get(bodyBytes);
                remotingMessage.setBody(bodyBytes);
            }
            return remotingMessage;
        }
        //won't happen for now
        return remotingMessage;
    }

}
