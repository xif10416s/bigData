package org.fxi.test.net.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created by seki on 17/8/16.
 */
public class Client {
    public static void main(String[] args){
        try {
            Socket socket = new Socket("localhost", 12345);

            System.out.println("Returns the local port number :" + socket.getLocalPort());
            System.out.println("remote port number : " + socket.getPort());

            // 2、获取输出流，向服务器端发送信息
            PrintWriter write = new PrintWriter(socket.getOutputStream());
            //3、获取输入流，并读取服务器端的响应信息
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            write.println("hi server...");
            write.flush();

            System.out.println("receive Server:" + in.readLine());

            Thread.sleep(10000L);


            write.close();
            in.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
