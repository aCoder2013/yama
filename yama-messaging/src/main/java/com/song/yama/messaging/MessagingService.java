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

package com.song.yama.messaging;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public interface MessagingService extends AutoCloseable {

    void start() throws InterruptedException;

    boolean isRunning();

    Address address();

    byte[] send(Address to, String subject, byte[] message) throws InterruptedException;

    byte[] send(Address to, String subject, byte[] message, Duration timeout) throws InterruptedException;

    CompletableFuture<byte[]> sendAsync(Address address, String subject, byte[] message);

    void sendOneway(Address to, String subject, byte[] message);

    void sendOneway(Address address, String subject, byte[] message, Duration timeout);

    CompletableFuture<Void> sendOnewayAsync(Address to, String subject, byte[] message);

    void registerMessageHandler(String subject, BiConsumer<Address, byte[]/*response */> handler);

    void registerMessageHandler(String subject, BiConsumer<Address, byte[]/*response*/> handler, Executor executor);

    void registerMessageHandler(String subject, BiFunction<Address, byte[]/*message */, byte[]/*response*/> handler);

    void registerMessageHandler(String subject, BiFunction<Address, byte[]/*message*/, byte[]/*response*/> handler,
        Executor executor);

}
