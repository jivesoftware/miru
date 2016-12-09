package com.jivesoftware.os.miru.logappender;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.ReadTimeoutHandler;

import java.io.IOException;
import java.util.List;

class NettyHttpPoster implements MiruLogSender {

    private static final String path = "/miru/stumptown/intake";
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final String host;
    private final int port;
    private final boolean sslEnabled;
    private final int soTimeoutSeconds;

    private EventLoopGroup group = new NioEventLoopGroup();
    private Bootstrap bootstrap;
    private Channel channel;

    private SslContext sslContext;

    NettyHttpPoster(String host, int port, boolean sslEnabled, long soTimeout) throws IOException {
        this.host = host;
        this.port = port;
        this.sslEnabled = sslEnabled;
        this.soTimeoutSeconds = (int) (soTimeout / 1000);

        if (this.sslEnabled) {
            sslContext = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        }
    }

    class ClientExceptionHandler extends SimpleChannelInboundHandler<String> {
        @Override
        public void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
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

                        if (sslEnabled) {
                            pipeline.addLast(sslContext.newHandler(ch.alloc()));
                        }

                        pipeline.addLast(new ReadTimeoutHandler(soTimeoutSeconds));
                        pipeline.addLast(new HttpClientCodec());
                        pipeline.addLast(new ClientExceptionHandler());
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

        // all netty io operations are asynchronous
        ChannelFuture channelFuture = channel.writeAndFlush(request);
        channelFuture.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
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
