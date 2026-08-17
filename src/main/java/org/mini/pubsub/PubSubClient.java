package org.mini.pubsub;

import com.google.protobuf.ByteString;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import org.mini.pubsub.client.ClientConfig;
import org.mini.pubsub.client.ClientHandler;
import org.mini.pubsub.client.ClientHeartbeatHandler;
import org.mini.pubsub.proto.PubSubProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PubSubClient {
    private static final Logger log = LoggerFactory.getLogger(PubSubClient.class);
    private final ClientConfig config;
    private final Map<String, MessageListener> listeners = new ConcurrentHashMap<>();
    private EventLoopGroup group;
    private Bootstrap bootstrap;
    private volatile Channel channel;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isUserClosed = new AtomicBoolean(false);
    private final AtomicInteger currentAddressIndex = new AtomicInteger(0);
    private volatile String connectedHost;
    private volatile Integer connectedPort;

    public PubSubClient(String host, int port) {
        this(new ClientConfig(host, port));
    }

    public PubSubClient(ClientConfig config) {
        this.config = config;
    }

    /**
     * Khởi tạo kết nối tới Pub-Sub Server.
     */
    public synchronized void connect() throws InterruptedException {
        if (isConnected.get()) return;

        if (group == null) {
            group = new NioEventLoopGroup();
            bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMs())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // 1. Decoders & Encoders
                            pipeline.addLast(new ProtobufVarint32FrameDecoder());
                            pipeline.addLast(new ProtobufDecoder(PubSubProto.MessageResponse.getDefaultInstance()));
                            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
                            pipeline.addLast(new ProtobufEncoder());

                            // 2. IdleStateHandler & Heartbeat
                            pipeline.addLast(new IdleStateHandler(0, 20, 0, TimeUnit.SECONDS));
                            pipeline.addLast(new ClientHeartbeatHandler());

                            // 3. ClientHandler
                            pipeline.addLast(new ClientHandler(PubSubClient.this, listeners));
                        }
                    });
        }

        if (config.getIsClusterMode()) {
            List<InetSocketAddress> addresses = config.getServerAddresses();
            int maxAttempts = (addresses != null && !addresses.isEmpty()) ? addresses.size() : 1;

            for (int i = 0; i < maxAttempts; i++) {
                InetSocketAddress targetAddress = getNextAddress();
                log.info("Connecting to Pub-Sub Server at {}:{}", targetAddress.getHostString(),
                        targetAddress.getPort());

                try {
                    ChannelFuture future = bootstrap.connect(targetAddress.getHostString(), targetAddress.getPort()).sync();
                    this.channel = future.channel();
                    this.isConnected.set(true);
                    this.isUserClosed.set(false);
                    this.connectedHost = targetAddress.getHostString();
                    this.connectedPort = targetAddress.getPort();

                    log.info("Successfully connected to Pub-Sub Server at {}:{}", targetAddress.getHostString(),
                            targetAddress.getPort());
                    return;
                } catch (Exception e) {
                    log.warn("Failed to connect to node {}:{}. Trying next node in cluster...",
                            targetAddress.getHostString(), targetAddress.getPort());
                }
            }

            if (config.isAutoReconnect()) {
                log.warn("Could not connect to any cluster node on startup. Scheduling background reconnect...");
                scheduleReconnect(0);
            } else {
                log.warn("Could not connect to any cluster node on startup");
            }
        } else {
            ChannelFuture future = bootstrap.connect(config.getHost(), config.getPort()).sync();
            this.channel = future.channel();
            this.isConnected.set(true);
            this.isUserClosed.set(false);
            this.connectedHost = config.getHost();
            this.connectedPort = config.getPort();

            log.info("Successfully connected to Pub-Sub Server at {}:{}", config.getHost(), config.getPort());
        }
    }

    /**
     * Lấy địa chỉ tiếp theo trong cụm
     */
    private InetSocketAddress getNextAddress() {
        List<InetSocketAddress> addresses = config.getServerAddresses();
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalStateException("No server addresses configured.");
        }
        int index = (currentAddressIndex.getAndIncrement() & 0x7FFFFFFF) % addresses.size();
        return addresses.get(index);
    }

    /**
     * Lập lịch Reconnect với Exponential Backoff khi bị rớt mạng.
     */
    public void scheduleReconnect(int attempts) {
        // Cập nhật lại trạng thái ngắt kết nối
        this.isConnected.set(false);

        // Nếu client chủ động shutdown thì dừng reconnect
        if (!config.isAutoReconnect() || isUserClosed.get()) return;

        long delay = Math.min(1L << attempts, 32);
        log.warn("Connection lost. Scheduling reconnect attempt #{} in {}s...", attempts + 1, delay);

        group.schedule(() -> {
            try {
                connect();
                resubscribeAll();
                log.info("Reconnected to {}:{} successfully!", connectedHost, connectedPort);
            } catch (Exception e) {
                log.error("Reconnect attempt #{} failed: {}", attempts + 1, e.getMessage());
                scheduleReconnect(attempts + 1);
            }
        }, delay, TimeUnit.SECONDS);
    }

    private void resubscribeAll() {
        for (String topic : listeners.keySet()) {
            PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                    .setType(PubSubProto.CommandType.SUBSCRIBE)
                    .setTopic(topic)
                    .build();
            channel.writeAndFlush(request);
            log.info("Resubscribed to topic [{}]", topic);
        }
    }

    public void subscribe(String topic, MessageListener listener) {
        checkConnection();
        listeners.put(topic, listener);

        PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                .setType(PubSubProto.CommandType.SUBSCRIBE)
                .setTopic(topic)
                .build();

        channel.writeAndFlush(request);
        log.info("Subscribed to topic [{}]", topic);
    }

    public void unsubscribe(String topic) {
        checkConnection();
        listeners.remove(topic);

        PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                .setType(PubSubProto.CommandType.UNSUBSCRIBE)
                .setTopic(topic)
                .build();

        channel.writeAndFlush(request);
        log.info("Unsubscribed from topic [{}]", topic);
    }

    public void publish(String topic, String message) {
        this.publish(topic, message.getBytes(StandardCharsets.UTF_8));
    }

    public void publish(String topic, byte[] payload) {
        checkConnection();

        PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                .setType(PubSubProto.CommandType.PUBLISH)
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .build();

        channel.writeAndFlush(request);
    }

    public synchronized void close() {
        isUserClosed.set(true);
        isConnected.set(false);

        try {
            if (channel != null) {
                channel.close().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (group != null) {
                group.shutdownGracefully();
            }
            log.info("Disconnected from Pub-Sub Server");
        }
    }

    private void checkConnection() {
        if (!isConnected.get() || channel == null || !channel.isActive()) {
            throw new IllegalStateException("PubSubClient is not connected to server");
        }
    }
}