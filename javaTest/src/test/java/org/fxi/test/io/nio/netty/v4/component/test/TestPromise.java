package org.fxi.test.io.nio.netty.v4.component.test;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import org.fxi.test.io.nio.netty.v4.simplechat.SimpleChatClientHandler;

import java.net.InetSocketAddress;

/**
 * Created by seki on 17/3/25.
 */
public class TestPromise {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("test start..." + Thread.currentThread().getName());
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        final DefaultChannelPromise promise = new DefaultChannelPromise(new NioSocketChannel());
        System.out.println("add listener start..." + Thread.currentThread().getName());
        promise.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                System.out.println("operationComplete ..." + Thread.currentThread().getName());
            }
        });
        System.out.println("add listener end..." + Thread.currentThread().getName());
        System.out.println("execute new task start..." + Thread.currentThread().getName());
        bossGroup.register(promise.channel());
        bossGroup.next().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                    System.out.println("execute new task channel connected..." + Thread.currentThread().getName());
                    promise.trySuccess();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println("execute new task end..." + Thread.currentThread().getName());
        System.out.println("main thread end..." + Thread.currentThread().getName());
        promise.sync();
    }
}
