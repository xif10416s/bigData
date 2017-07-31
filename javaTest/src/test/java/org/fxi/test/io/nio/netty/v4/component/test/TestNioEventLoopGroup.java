package org.fxi.test.io.nio.netty.v4.component.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.fxi.test.io.nio.netty.v4.simplechat.SimpleChatClientHandler;

/**
 * Created by seki on 17/3/24.
 */
public class TestNioEventLoopGroup {
	public static void main(String[] args) throws InterruptedException, IOException {
		EventLoopGroup bossGroup = new NioEventLoopGroup();

		final NioSocketChannel nioSocketChannel = new NioSocketChannel();
		ChannelFuture regFuture = bossGroup.next().register(nioSocketChannel);

		final PendingRegistrationPromise promise = new PendingRegistrationPromise(regFuture.channel());
		regFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				// Direclty obtain the cause and do a null check so we only need
				// one volatile read in case of a
				// failure.
				Throwable cause = future.cause();
				if (cause != null) {
					// Registration on the EventLoop failed so fail the
					// ChannelPromise directly to not cause an
					// IllegalStateException once we try to access the EventLoop
					// of the Channel.
				} else {
					promise.registered();
					System.out.println("----");
					final Channel channel = promise.channel();
                    final ChannelPipeline pipeline = channel.pipeline();

                    pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                    pipeline.addLast("decoder", new StringDecoder());
                    pipeline.addLast("encoder", new StringEncoder());
                    pipeline.addLast("handler", new SimpleChatClientHandler());
					channel.eventLoop().execute(new Runnable() {
						@Override
						public void run() {
							channel.connect(new InetSocketAddress(8080), promise);
						}
					});
				}
			}
		});

        Channel channel = promise.sync().channel();
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            channel.writeAndFlush(in.readLine() + "\r\n");
        }

    }
}

final class PendingRegistrationPromise extends DefaultChannelPromise {

	// Is set to the correct EventExecutor once the registration was successful.
	// Otherwise it will
	// stay null and so the GlobalEventExecutor.INSTANCE will be used for
	// notifications.
	private volatile boolean registered;

	PendingRegistrationPromise(Channel channel) {
		super(channel);
	}

	void registered() {
		registered = true;
	}

	@Override
	protected EventExecutor executor() {
		if (registered) {
			// If the registration was a success executor is set.
			//
			// See https://github.com/netty/netty/issues/2586
			return super.executor();
		}
		// The registration failed so we can only use the GlobalEventExecutor as
		// last resort to notify.
		return GlobalEventExecutor.INSTANCE;
	}
}
