package org.fxi.test.io.nio.netty.v4;

import io.netty.buffer.ByteBuf;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

/**
 * 处理服务端 channel.
 */
public class DiscardServerHandler extends ChannelInboundHandlerAdapter { // (1)

	// ChannelHandlerContext 对象提供了许多操作，使你能够触发各种各样的 I/O 事件和操作。这里我们调用了
	// write(Object) 方法来逐字地把接受到的消息写入。
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
		// 默默地丢弃收到的数据
		ByteBuf in = (ByteBuf) msg;
		// System.out.println((char) in.readByte());
		// System.out.flush();
		// ctx.write(Object) 方法不会使消息写入到通道上，他被缓冲在了内部，你需要调用 ctx.flush()
		// 方法来把缓冲区中数据强行输出。
		// 或者你可以用更简洁的 cxt.writeAndFlush(msg) 以达到同样的目的。
		ctx.write(msg); // (1)
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
		// 当出现异常就关闭连接
		cause.printStackTrace();
		ctx.close();
	}
}