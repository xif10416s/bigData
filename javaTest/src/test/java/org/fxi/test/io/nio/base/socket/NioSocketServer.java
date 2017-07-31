package org.fxi.test.io.nio.base.socket;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;

public class NioSocketServer {
	private int ports[] = new int[1];
	private ByteBuffer echoBuffer = ByteBuffer.allocate(1024);

	public NioSocketServer(int ports[]) throws IOException {
		this.ports = ports;

		go();
	}

	private void go() throws IOException {
		// 创建一个 Selector：
		// Selector 就是您注册对各种 I/O 事件的兴趣的地方，而且当那些事件发生时，就是这个对象告诉您所发生的事件。
		Selector selector = Selector.open();

		// Open a listener on each port, and register each one
		// with the selector
		ServerSocketChannel ssc = ServerSocketChannel.open();
		ssc.configureBlocking(false);
		ServerSocket ss = ssc.socket();
		InetSocketAddress address = new InetSocketAddress(ports[0]);
		ss.bind(address);
		// OP_ACCEPT在新的连接建立时所发生的事件。这是适用于 ServerSocketChannel 的唯一事件类型。
		// SelectionKey 代表这个通道在此 Selector 上的这个注册。
        ssc.register(selector, SelectionKey.OP_ACCEPT);

		System.out.println("Going to listen on " + ports[0]);

		while (true) {
			// 调用 Selector 的 select() 方法。这个方法会阻塞，直到至少有一个已注册的事件发生。
			int num = selector.select();
            System.out.println("begin select num  " + num);
			Set selectedKeys = selector.selectedKeys();
			Iterator it = selectedKeys.iterator();

			while (it.hasNext()) {
				SelectionKey key = (SelectionKey) it.next();

				if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
					// Accept the new connection
					ServerSocketChannel ssc2 = (ServerSocketChannel) key.channel();
					SocketChannel sc = ssc2.accept();
					sc.configureBlocking(false);

					// Add the new connection to the selector
					SelectionKey newKey = sc.register(selector, SelectionKey.OP_READ);
					it.remove();

					System.out.println("Got connection from " + sc);
				} else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
					// Read the data
					SocketChannel sc = (SocketChannel) key.channel();

					// Echo data
					int bytesEchoed = 0;
					while (true) {
						echoBuffer.clear();

						int r = sc.read(echoBuffer);

						if (r <= 0) {
							break;
						}

						echoBuffer.flip();

						bytesEchoed += r;
					}

					sc.write(ByteBuffer.wrap(new String("from server" + new Date())
							.getBytes()));
					System.out.println("Echoed ==>" + Charset.forName("UTF-8").decode(echoBuffer).toString() + " from " + sc.getLocalAddress());
					sc.register(selector, SelectionKey.OP_READ);
					it.remove();
				}

			}

			// System.out.println( "going to clear" );
			// selectedKeys.clear();
			// System.out.println( "cleared" );
		}
	}

	static public void main(String args[]) throws Exception {
		new NioSocketServer(new int[]{8888});
	}
}
