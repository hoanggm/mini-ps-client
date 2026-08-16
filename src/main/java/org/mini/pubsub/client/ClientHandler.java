package org.mini.pubsub.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.mini.pubsub.MessageListener;
import org.mini.pubsub.proto.PubSubProto;

import java.util.Map;

public class ClientHandler extends SimpleChannelInboundHandler<PubSubProto.MessageResponse> {
    private final Map<String, MessageListener> listeners;

    public ClientHandler(Map<String, MessageListener> listeners) {
        this.listeners = listeners;
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

                    // auto send ack
                    response.getMessageId();
                    if (!response.getMessageId().isEmpty()) {
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
