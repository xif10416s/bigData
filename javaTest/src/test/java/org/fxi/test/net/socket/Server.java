package org.fxi.test.net.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by seki on 17/8/16.
 */
public class Server {
    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(12345,2);
            for(int i = 0 ; i<3 ; i++) {
               final Socket socket = serverSocket.accept();

                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            System.out.println("Returns the local port number :" + socket.getLocalPort());
                            System.out.println("remote port number : " + socket.getPort());
                            // 3、获取输入流，并读取客户端信息
                            String line;
                            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                            // 由Socket对象得到输入流，并构造相应的BufferedReader对象
                            PrintWriter writer = new PrintWriter(socket.getOutputStream());
                            // 由Socket对象得到输出流，并构造PrintWriter对象
                            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                            // 由系统标准输入设备构造BufferedReader对象
                            line = in.readLine();
                            System.out.println("Client:" + line);
                            // 在标准输出上打印从客户端读入的字符串

                            writer.println("server resp: " + line);
                            // 向客户端输出该字符串
                            writer.flush();
                            in.close();
                            writer.close();
                            socket.close();
                        }catch (Exception e){
                            e.printStackTrace();;
                        }
                    }
                }).start();
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
