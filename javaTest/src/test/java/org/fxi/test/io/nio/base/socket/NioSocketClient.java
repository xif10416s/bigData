package org.fxi.test.io.nio.base.socket;

/**
 * Created by seki on 17/2/25.
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;

public class NioSocketClient {

    public static void main(String args[]) throws IOException{

        SocketChannel channel = SocketChannel.open(); //打开Channel
        channel.configureBlocking(false);
        channel.connect(new InetSocketAddress(8888)); //连接

        Selector selector = Selector.open();    //打开Selector
        channel.register(selector, SelectionKey.OP_CONNECT|SelectionKey.OP_READ); //注册OP_CONNECT

        while (true) {
            selector.select();  //	轮询
            System.out.println("client on selected...");
            Iterator iterator = selector.selectedKeys().iterator(); //获取可读的
            while (iterator.hasNext()) {

                SelectionKey key = (SelectionKey) iterator.next();
                iterator.remove();

                Handle(key, selector);
            }

//            try {
//                Thread.sleep(50000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public static void Handle(SelectionKey key, Selector sel)
            throws IOException {

        SocketChannel client = (SocketChannel) key.channel();

        if (client.isConnectionPending()) {
            if (client.finishConnect()) {

                ByteBuffer byteBuffer = ByteBuffer.allocate(200);
                byteBuffer = ByteBuffer.wrap(new String("from client" + new Date())
                        .getBytes());
                client.write(byteBuffer);
                client.register(sel, SelectionKey.OP_READ);
            }
        } else {
            if (key.isReadable()) {
                ByteBuffer echoBuffer = ByteBuffer.allocate(1024);
                SocketChannel sc = (SocketChannel) key.channel();
                sc.read(echoBuffer);
                echoBuffer.flip();

                System.out.println("echo server return:" + Charset.forName("UTF-8").decode(echoBuffer).toString());
                echoBuffer.clear();
                client.write(ByteBuffer.wrap(new String("from client" + new Date())
                        .getBytes()));
            }
        }

    }
}