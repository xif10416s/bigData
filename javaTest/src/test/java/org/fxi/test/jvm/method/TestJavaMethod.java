package org.fxi.test.jvm.method;

import java.nio.ByteBuffer;

/**
 * Created by seki on 17/3/8.
 */
public class TestJavaMethod {
    public static void main(String[] args) throws InterruptedException {
        Thread.sleep(10000);
        getBuffer();
        System.out.println("111");

    }

    public static void getBuffer() throws InterruptedException {
        ByteBuffer allocate = ByteBuffer.allocate(1024 * 1000);
        Thread.sleep(100000);
    }
}
