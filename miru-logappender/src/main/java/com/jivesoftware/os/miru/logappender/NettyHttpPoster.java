package com.jivesoftware.os.miru.logappender;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.IOException;
import java.util.List;

class NettyHttpPoster implements MiruLogSender {

    private static final String path = "/miru/stumptown/intake";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String host;
    private final int port;
    private final int soTimeoutSeconds;

    private EventLoopGroup group = new NioEventLoopGroup();
    private Bootstrap bootstrap;
    private Channel channel;

    NettyHttpPoster(String host, int port, long soTimeout) throws IOException {
        this.host = host;
        this.port = port;
        this.soTimeoutSeconds = (int) (soTimeout / 1000);
    }

    private void connect() throws IOException, InterruptedException {
        bootstrap = new Bootstrap();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ReadTimeoutHandler(soTimeoutSeconds));
                        pipeline.addLast(new HttpClientCodec());
                    }
                });

        channel = bootstrap.connect(host, port).sync().channel();
    }

    private void post(byte[] data) throws InterruptedException {
        FullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                path);

        request.headers().set(HttpHeaderNames.HOST, host);
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);

        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, data.length);
        request.content().writeBytes(data);

        channel.writeAndFlush(request);
    }

    void close() throws InterruptedException {
        channel.closeFuture().sync();
        group.shutdownGracefully();

        bootstrap = null;
        channel = null;
    }

    @Override
    public void send(List<MiruLogEvent> events) throws Exception {
        if (channel == null) {
            connect();
        }

        byte[] jsonBytes = objectMapper.writeValueAsBytes(events);

        try {
            post(jsonBytes);
        } catch (InterruptedException x) {
            close();
            throw x;
        } catch (Exception x) {
            System.err.println("Failed to log append sizeInBytes:" + jsonBytes.length + " to http://" + host + ":" + port + "" + path + " ");
            x.printStackTrace();
            close();
            throw x;
        }
    }

}
