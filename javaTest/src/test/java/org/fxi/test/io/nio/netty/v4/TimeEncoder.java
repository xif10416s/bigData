package org.fxi.test.io.nio.netty.v4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by seki on 17/2/28.
 */
public class TimeEncoder extends MessageToByteEncoder<UnixTime> {
    @Override
    protected void encode(ChannelHandlerContext ctx, UnixTime msg, ByteBuf out) {
        out.writeInt((int)msg.value());
    }
}
/**
 *
 *
 * public class TimeEncoder extends MessageToByteEncoder<UnixTime> {
@Override
protected void encode(ChannelHandlerContext ctx, UnixTime msg, ByteBuf out) {
out.writeInt((int)msg.value());
}
}
 */
