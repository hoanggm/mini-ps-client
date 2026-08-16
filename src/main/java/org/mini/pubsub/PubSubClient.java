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
import org.mini.pubsub.client.ClientConfig;
import org.mini.pubsub.client.ClientHandler;
import org.mini.pubsub.proto.PubSubProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PubSubClient {
    private static final Logger log = LoggerFactory.getLogger(PubSubClient.class);
    private final ClientConfig config;
    private final Map<String, MessageListener> listeners = new ConcurrentHashMap<>();

    private EventLoopGroup group;
    private Channel channel;
    private volatile boolean isConnected = false;

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
        if (isConnected) return;

        group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMs())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        // Frame & Protobuf Codecs
                        p.addLast(new ProtobufVarint32FrameDecoder());
                        p.addLast(new ProtobufDecoder(PubSubProto.MessageResponse.getDefaultInstance()));
                        p.addLast(new ProtobufVarint32LengthFieldPrepender());
                        p.addLast(new ProtobufEncoder());
                        // Handler nội bộ xử lý response
                        p.addLast(new ClientHandler(listeners));
                    }
                });

        ChannelFuture future = bootstrap.connect(config.getHost(), config.getPort()).sync();
        this.channel = future.channel();
        this.isConnected = true;

        log.info("Successfully connected to Pub-Sub Server at {}:{}", config.getHost(), config.getPort());
    }

    /**
     * Đăng ký nhận tin từ một Topic.
     */
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

    /**
     * Hủy đăng ký Topic.
     */
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

    /**
     * Gửi (Publish) tin nhắn dữ liệu dạng String.
     */
    public void publish(String topic, String message) {
        this.publish(topic, message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Gửi (Publish) tin nhắn dữ liệu mảng byte.
     */
    public void publish(String topic, byte[] payload) {
        checkConnection();

        PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                .setType(PubSubProto.CommandType.PUBLISH)
                .setTopic(topic)
                .setPayload(ByteString.copyFrom(payload))
                .build();

        channel.writeAndFlush(request);
    }

    /**
     * Ngắt kết nối và giải phóng tài nguyên.
     */
    public synchronized void close() {
        if (!isConnected) return;

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
            isConnected = false;
            log.info("Disconnected from Pub-Sub Server");
        }
    }

    private void checkConnection() {
        if (!isConnected || channel == null || !channel.isActive()) {
            throw new IllegalStateException("PubSubClient is not connected to server");
        }
    }

    /**
     * Hàm xử lý Reconnect được gọi tự động bởi ClientHandler
     */
    public synchronized void reconnect() {
        if (!isConnected) {
            log.info("Trying reconnect to {}:{}", config.getHost(), config.getPort());
            try {
                this.connect();
                for (String topic : listeners.keySet()) {
                    PubSubProto.MessageRequest request = PubSubProto.MessageRequest.newBuilder()
                            .setType(PubSubProto.CommandType.SUBSCRIBE)
                            .setTopic(topic)
                            .build();
                    channel.writeAndFlush(request);
                }
                log.info("Reconnect {}:{} successfully !!!", config.getHost(), config.getPort());
            } catch (Exception ignored) {
            }
        }
    }
}
