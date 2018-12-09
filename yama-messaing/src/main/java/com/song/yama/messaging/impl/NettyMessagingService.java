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

package com.song.yama.messaging.impl;

import com.google.common.collect.Lists;
import com.song.yama.common.utils.FutureResult;
import com.song.yama.common.utils.MathUtils;
import com.song.yama.common.utils.ThreadUtils;
import com.song.yama.messaging.Address;
import com.song.yama.messaging.MessagingService;
import com.song.yama.messaging.codec.MessageDecoder;
import com.song.yama.messaging.codec.MessageEncoder;
import com.song.yama.messaging.exception.MessagingException;
import com.song.yama.messaging.exception.SendResponseException;
import com.song.yama.messaging.message.RemotingMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NettyMessagingService implements MessagingService {

    private static final int MAX_FRAME_LENGTH = 5 * 1024 * 1024; //5K
    private static final int DEFAULT_CHANNEL_POOL_SIZE = 8;
    private static final int DEFAULT_SEND_MESSAGE_TIMEOUT_SECOND = 6;

    private final Address address;

    private final AtomicReference<State> stateReference = new AtomicReference<>(State.UNINITIALIZED);

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Class<? extends ServerChannel> serverChannelClass;
    private Class<? extends Channel> clientChannelClass;
    private ScheduledExecutorService timeoutExecutor;
    private Channel serverChannel;

    private final ConcurrentMap<Address, List<CompletableFuture<Channel>>> channels = new ConcurrentHashMap<>(256);

    private final ConcurrentMap<Integer/*requestId*/, CompletableFuture<RemotingMessage>> responseMap = new ConcurrentHashMap<>(
        256);

    public NettyMessagingService(Address address) {
        this.address = address;
    }

    @Override
    public void start() throws InterruptedException {
        if (stateReference.compareAndSet(State.UNINITIALIZED, State.STARTING)) {
            initNettyServer();
            initScheduleTasks();
        } else {
            State state = stateReference.get();
            if (state == State.STARTED) {
                log.warn("MessagingService is already started : " + Thread.currentThread().getName());
            } else if (state == State.CLOSING || state == State.CLOSED) {
                throw new IllegalStateException("NettyMessagingService is closing or already closed :" + state);
            } else if (state == State.STARTING) {
                throw new IllegalStateException("NettyMessagingService is still starting");
            } else {
                throw new IllegalStateException("Current state of NettyMessagingService is :" + state);
            }
        }
    }

    private void initEventLoopGroup() {
        try {
            clientGroup = new EpollEventLoopGroup(0,
                ThreadUtils.newThreadFactory("netty-messaging-client-pool-%d", log));
            serverGroup = new EpollEventLoopGroup(0,
                ThreadUtils.newThreadFactory("netty-messaging-server-pool-%d", log));
            serverChannelClass = EpollServerSocketChannel.class;
            clientChannelClass = EpollSocketChannel.class;
        } catch (Throwable e) {
            //fallback to nio
            log.info("Failed to initialize native epoll transport," + "due to : {},fallback to nio.", e.getMessage());
            clientGroup = new NioEventLoopGroup(0, ThreadUtils.newThreadFactory("netty-messaging-client-pool-%d", log));
            serverGroup = new NioEventLoopGroup(0, ThreadUtils.newThreadFactory("netty-messaging-server-pool-%d", log));
            serverChannelClass = NioServerSocketChannel.class;
            clientChannelClass = NioSocketChannel.class;
        }
    }

    private void initNettyServer() throws InterruptedException {
        initEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_REUSEADDR, true);
        b.option(ChannelOption.SO_BACKLOG, 128);
        b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024, 32 * 1024));
        b.childOption(ChannelOption.SO_RCVBUF, 1024 * 1024);
        b.childOption(ChannelOption.SO_SNDBUF, 1024 * 1024);
        b.childOption(ChannelOption.SO_KEEPALIVE, true);
        b.childOption(ChannelOption.TCP_NODELAY, true);
        b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        b.group(serverGroup, clientGroup);
        b.channel(serverChannelClass);
        b.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                channel.pipeline()
                    .addLast(new MessageEncoder())
                    .addLast(new MessageDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                    .addLast(new MessageHandler());
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> throwable = new AtomicReference<>(null);

        b.bind(this.address.getPort()).addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                log.info("{} accepting incoming connections on port {}", this.address.getHost(),
                    this.address.getPort());
                serverChannel = f.channel();
                latch.countDown();
            } else {
                log.warn("{} failed to bind to port {} due to {}", this.address.getHost(), this.address.getPort(),
                    f.cause());
                throwable.set(f.cause());
                latch.countDown();
            }
        });
        latch.await(5, TimeUnit.SECONDS);
        if (throwable.get() != null) {
            throw new MessagingException("Failed to bind to port :" + this.address.getPort(), throwable.get());
        }
    }

    private void initScheduleTasks() {
        this.timeoutExecutor = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.newThreadFactory("netty-messaging-timeoutChecker-%d", log));
    }

    @Override
    public boolean isRunning() {
        return this.stateReference.get() == State.STARTED;
    }

    @Override
    public Address address() {
        return this.address;
    }

    @Override
    public byte[] send(Address to, String subject, byte[] message) throws InterruptedException {
        return send(to, subject, message, Duration.ofSeconds(DEFAULT_SEND_MESSAGE_TIMEOUT_SECOND));
    }

    @Override
    public byte[] send(Address to, String subject, byte[] message, Duration timeout) throws InterruptedException {
        CompletableFuture<Channel> createChannelFuture = getOrCreateChannel(to, subject);
        CountDownLatch latch = new CountDownLatch(1);
        FutureResult<byte[]> futureResult = new FutureResult<>();
        createChannelFuture.thenAccept(channel -> {
            RemotingMessage remotingMessage = RemotingMessage.builder()
                .withCode(0)
                .withSubject(subject)
                .withBody(message)
                .build();
            CompletableFuture<RemotingMessage> responseFuture = new CompletableFuture<>();
            int requestId = remotingMessage.getRequestId();
            responseMap.put(requestId, responseFuture);
            channel.writeAndFlush(remotingMessage.encode()).addListener(future -> {
                if (future.isSuccess()) {
                    //TODO:timeout response scheduled check
                    responseFuture.whenComplete((responseMessage, throwable) -> {
                        if (throwable != null) {
                            futureResult.setThrowable(throwable);
                        } else {
                            if (remotingMessage.getCode() != 0) {
                                futureResult.setThrowable(new SendResponseException(
                                    String
                                        .format("[%s:%d] Process message failed with code [%d]:%s.", address.getHost(),
                                            address.getPort(), remotingMessage.getCode(),
                                            remotingMessage.getMessage())));
                            } else {
                                futureResult.setData(remotingMessage.getBody());
                            }
                        }
                        responseMap.remove(requestId);
                        latch.countDown();
                    });
                } else {
                    futureResult.setThrowable(future.cause());
                    latch.countDown();
                }
            });
        }).exceptionally(throwable -> {
            futureResult.setThrowable(throwable);
            return null;
        });
        latch.await(timeout.getSeconds(), TimeUnit.SECONDS);
        if (futureResult.getThrowable() != null) {
            throw new MessagingException("Send message failed.", futureResult.getThrowable());
        }
        return futureResult.getData();
    }

    @Override
    public CompletableFuture<byte[]> sendAsync(Address address, String subject, byte[] message) {
        return null;
    }

    @Override
    public void sendOneway(Address to, String subject, byte[] message) {

    }

    @Override
    public void sendOneway(Address address, String subject, byte[] message, Duration timeout) {

    }

    @Override
    public CompletableFuture<Void> sendOnewayAsync(Address to, String subject, byte[] message) {
        return null;
    }

    @Override
    public void registerMessageHandler(String subject, BiConsumer<Address, byte[]> handler) {

    }

    @Override
    public void registerMessageHandler(String subject, BiConsumer<Address, byte[]> handler, Executor executor) {

    }

    @Override
    public void registerMessageHandler(String subject, BiFunction<Address, byte[], byte[]> handler) {

    }

    @Override
    public void registerMessageHandler(String subject, BiFunction<Address, byte[], byte[]> handler, Executor executor) {

    }

    @Override
    public void close() throws Exception {

    }

    public CompletableFuture<Channel> getOrCreateChannel(Address address, String subject) {
        List<CompletableFuture<Channel>> channelPool = getOrCreateChannelPool(address);
        int index = MathUtils.safeMod(subject.hashCode(), DEFAULT_CHANNEL_POOL_SIZE);
        CompletableFuture<Channel> channelFuture = channelPool.get(index);
        if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
            synchronized (channels) {
                channelFuture = channelPool.get(index);
                if (channelFuture == null || channelFuture.isCompletedExceptionally()) {
                    channelFuture = openConnection(address);
                    channelPool.set(index, channelFuture);
                }
            }
        }

        CompletableFuture<Channel> future = new CompletableFuture<>();
        channelFuture.thenAccept(channel -> {
            if (channel.isActive()) {
                future.complete(channel);
            } else {
                synchronized (channels) {
                    channelPool.remove(index);
                }
                future.completeExceptionally(new IllegalStateException(String
                    .format("Connect to remote host [%s:%d] failed : %s.", address.getHost(), address.getPort(),
                        channel.toString())));
            }
        }).exceptionally(throwable -> {
            future.completeExceptionally(throwable);
            return null;
        });
        return future;
    }

    public List<CompletableFuture<Channel>> getOrCreateChannelPool(Address address) {
        List<CompletableFuture<Channel>> channelPoolsFuture = channels.get(address);
        if (channelPoolsFuture != null) {
            return channelPoolsFuture;
        }

        return channels.computeIfAbsent(address, addr -> {
            List<CompletableFuture<Channel>> pool = new ArrayList<>(DEFAULT_CHANNEL_POOL_SIZE);
            for (int i = 0; i < DEFAULT_CHANNEL_POOL_SIZE; i++) {
                pool.add(null);
            }
            return Lists.newCopyOnWriteArrayList(pool);
        });
    }

    /**
     * open a connection to client
     */
    private CompletableFuture<Channel> openConnection(Address address) {
        Bootstrap bootstrap = initBootstrapClient();
        CompletableFuture<Channel> future = new CompletableFuture<>();
        ChannelFuture channelFuture = bootstrap.connect();
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                future.complete(channelFuture.channel());
            } else {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    private Bootstrap initBootstrapClient() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
            new WriteBufferWaterMark(10 * 32 * 1024, 10 * 64 * 1024));
        bootstrap.option(ChannelOption.SO_RCVBUF, 1024 * 1024);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1024 * 1024);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000);
        bootstrap.group(clientGroup);
        bootstrap.channel(clientChannelClass);
        bootstrap.remoteAddress(address.getHost(), address.getPort());
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                    .addLast(new MessageEncoder())
                    .addLast(new MessageDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                    .addLast(new MessageHandler());
            }
        });
        return bootstrap;
    }


    public enum State {
        UNINITIALIZED,
        STARTING,
        STARTED,
        CLOSING,
        CLOSED
    }

    public class MessageHandler extends SimpleChannelInboundHandler<RemotingMessage> {

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {

        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingMessage msg) throws Exception {

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        }
    }

}
