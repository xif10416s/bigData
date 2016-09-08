package org.fxi.test.collection;

import java.util.ArrayList;

import org.junit.Test;

/**
 * Created by seki on 16/4/23.
 */
public class BaseTest {
    @Test
    public void test(){
        System.out.print("-----");
    }

    @Test
    public void testArray(){
        int[] a = new int [10];

    }

    /**
     * 1.array 实现 默认 长度10
     */
    @Test
    public void testList(){
        ArrayList<String> strings = new ArrayList<String>();
    }

}
