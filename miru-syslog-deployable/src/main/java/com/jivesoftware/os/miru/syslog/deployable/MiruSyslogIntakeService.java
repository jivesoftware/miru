package com.jivesoftware.os.miru.syslog.deployable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jivesoftware.os.miru.logappender.MiruLogEvent;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.routing.bird.http.client.HttpResponse;
import com.jivesoftware.os.routing.bird.http.client.RoundRobinStrategy;
import com.jivesoftware.os.routing.bird.http.client.TenantAwareHttpClient;
import com.jivesoftware.os.routing.bird.shared.ClientCall;
import com.jivesoftware.os.routing.bird.shared.NextClientStrategy;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

class MiruSyslogIntakeService {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final boolean enabled;
    private final int port;
    private final boolean tcpKeepAlive;
    private final int maxFrameLength;
    private final int receiveBufferSize;
    TenantAwareHttpClient<String> client;

    private final BlockingQueue<MiruLogEvent> queue;
    private final int batchSize;
    private final boolean blocking;
    private final long ifSuccessPauseMillis;
    private final long ifEmptyPauseMillis;
    private final long ifErrorPauseMillis;
    private final int nonBlockingDrainThreshold;
    private final int nonBlockingDrainCount;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private ExecutorService listenProcessor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("miru-syslog-listener-%d").build());
    private ExecutorService queueProcessor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("miru-syslog-queue-%d").build());

    private final NextClientStrategy nextClientStrategy = new RoundRobinStrategy();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final String endpoint = "/miru/stumptown/intake";

    MiruSyslogIntakeService(boolean enabled,
        int port,
        boolean tcpKeepAlive,
        int receiveBufferSize,
        int maxFrameLength,
        int queueSize,
        int batchSize,
        boolean blocking,
        long ifSuccessPauseMillis,
        long ifEmptyPauseMillis,
        long ifErrorPauseMillis,
        int nonBlockingDrainThreshold,
        int nonBlockingDrainCount,
        TenantAwareHttpClient<String> client) {
        this.enabled = enabled;

        this.port = port;
        this.tcpKeepAlive = tcpKeepAlive;
        this.receiveBufferSize = receiveBufferSize;
        this.maxFrameLength = maxFrameLength;

        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.batchSize = batchSize;
        this.blocking = blocking;
        this.ifSuccessPauseMillis = ifSuccessPauseMillis;
        this.ifEmptyPauseMillis = ifEmptyPauseMillis;
        this.ifErrorPauseMillis = ifErrorPauseMillis;
        this.nonBlockingDrainThreshold = nonBlockingDrainThreshold;
        this.nonBlockingDrainCount = nonBlockingDrainCount;

        this.client = client;
    }

    void start() {
        LOG.info("Port: {}", port);
        LOG.info("TCP Keep Alive: {}", tcpKeepAlive);
        LOG.info("Max Frame Length: {}", maxFrameLength);
        LOG.info("Receive Buffer Size: {}", receiveBufferSize);

        if (!enabled) {
            LOG.warn("Syslog service listener is not enabled.");
            return;
        }

        if (started.compareAndSet(false, true)) {
            queueProcessor.submit(new QueueConsumer());
            listenProcessor.submit(new SyslogListener());
        }
    }

    void stop() {
        if (started.compareAndSet(true, false)) {
            queueProcessor.shutdownNow();
            listenProcessor.shutdownNow();
        }
    }

    private class SyslogListener implements Runnable {

        @Sharable
        private class SyslogServerHandler extends SimpleChannelInboundHandler<String> {

            @Override
            public void channelRead0(ChannelHandlerContext ctx, String line) throws Exception {
                InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();

                SyslogEvent syslogEvent = new SyslogEvent()
                    .setMessage(line)
                    .setAddress(remoteAddress)
                    .build();
                LOG.info(syslogEvent.toString());

                if (syslogEvent.miruLogEvent == null) {
                    LOG.error("Syslog event not valid: {}", line);
                } else {
                    if (blocking) {
                        try {
                            queue.put(syslogEvent.miruLogEvent);
                        } catch (InterruptedException ie) {
                            LOG.error("Interrupted while waiting for a free space in syslog queue.");
                        }
                    } else {
                        if (queue.remainingCapacity() < nonBlockingDrainThreshold) {
                            LOG.error("Draining to create space in the syslog queue.");
                            queue.drainTo(DEV_NULL_COLLECTION, nonBlockingDrainCount);
                        }
                        if (!queue.offer(syslogEvent.miruLogEvent)) {
                            LOG.error("Syslog is unable to write to full syslog queue.");
                        }
                    }
                }
            }

        }

        @Override
        public void run() {
            EventLoopGroup parentGroup = new NioEventLoopGroup(1);
            EventLoopGroup childGroup = new NioEventLoopGroup();

            try {
                new ServerBootstrap().group(parentGroup, childGroup)
                    .option(ChannelOption.SO_RCVBUF, receiveBufferSize)
                    .option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new LineBasedFrameDecoder(maxFrameLength));
                            pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
                            pipeline.addLast(new SyslogServerHandler());
                        }
                    })
                    .bind(port).sync().channel().closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                parentGroup.shutdownGracefully();
                childGroup.shutdownGracefully();
            }
        }

    }

    private class QueueConsumer implements Runnable {

        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public void run() {
            List<MiruLogEvent> miruLogEvents = new ArrayList<>();

            while (running.get()) {
                queue.drainTo(miruLogEvents, batchSize);

                if (miruLogEvents.isEmpty()) {
                    try {
                        Thread.sleep(ifEmptyPauseMillis);
                    } catch (InterruptedException e) {
                        LOG.error("QueueConsumer was interrupted while sleeping due to empty queue.");
                        Thread.interrupted();
                    }
                } else {
                    while (true) {
                        try {
                            String toJson = objectMapper.writeValueAsString(miruLogEvents);

                            HttpResponse httpResponse = client.call(
                                "",
                                nextClientStrategy,
                                "ingress",
                                client -> new ClientCall.ClientResponse<>(client.postJson(endpoint, toJson, null), true));
                            if (httpResponse.getStatusCode() != 202) {
                                throw new Exception("Error [" + httpResponse.getStatusCode() + "] posting: " + toJson);
                            }

                            break;
                        } catch (Exception e) {
                            System.err.println("Append failed for logger: " + e.getClass().getCanonicalName() + ": " + e.getMessage());
                        }

                        try {
                            Thread.sleep(ifErrorPauseMillis);
                        } catch (InterruptedException e) {
                            LOG.error("QueueConsumer was interrupted while sleeping due to errors.");
                            Thread.interrupted();
                        }
                    }

                    miruLogEvents.clear();

                    try {
                        Thread.sleep(ifSuccessPauseMillis);
                    } catch (InterruptedException e) {
                        LOG.error("QueueConsumer was interrupted while sleeping after success.");
                        Thread.interrupted();
                    }
                }
            }
        }

        private void stop() {
            running.set(false);
        }

    }

    private static final DevNullCollection<MiruLogEvent> DEV_NULL_COLLECTION = new DevNullCollection<>();

    private static class DevNullCollection<E> implements Collection<E> {

        private DevNullCollection() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return Collections.EMPTY_LIST.iterator();
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return a;
        }

        @Override
        public boolean add(E e) {
            return true;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return true;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {
        }
    }

}
