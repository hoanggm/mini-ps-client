package org.mini.pubsub.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.mini.pubsub.proto.PubSubProto;

public class ClientHeartbeatHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent event) {
            if (event.state() == IdleState.WRITER_IDLE) {
                ctx.writeAndFlush(PubSubProto.MessageRequest.newBuilder()
                        .setType(PubSubProto.CommandType.PING)
                        .build());
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
