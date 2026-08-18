package org.mini.pubsub.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.mini.pubsub.MessageListener;
import org.mini.pubsub.PubSubClient;
import org.mini.pubsub.proto.PubSubProto;

import java.util.Map;
import java.util.Objects;

public class ClientHandler extends SimpleChannelInboundHandler<PubSubProto.MessageResponse> {
    private final PubSubClient client;
    private final Map<String, MessageListener> listeners;

    public ClientHandler(PubSubClient client, Map<String, MessageListener> listeners) {
        this.client = client;
        this.listeners = listeners;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        client.scheduleReconnect(0);
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PubSubProto.MessageResponse response) {
        String topic = response.getTopic();
        byte[] payload = response.getPayload().toByteArray();

        if (payload.length > 0) {
            MessageListener listener = listeners.get(topic);
            if (listener != null) {
                try {
                    listener.onMessage(topic, payload);

                    if (response.getMessageId() != 0L) {
                        PubSubProto.MessageRequest ackRequest = PubSubProto.MessageRequest.newBuilder()
                                .setType(PubSubProto.CommandType.ACK)
                                .setMessageId(response.getMessageId())
                                .setTopic(topic)
                                .build();

                        ctx.writeAndFlush(ackRequest);
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ctx.close();
    }
}