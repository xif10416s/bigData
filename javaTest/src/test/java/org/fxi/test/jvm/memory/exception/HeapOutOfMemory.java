package org.fxi.test.jvm.memory.exception;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**

 * @Described：堆溢出测试

 * @VM args:-verbose:gc -Xms2M -Xmx2M -XX:+PrintGCDetails
 */
public class HeapOutOfMemory {
    /**

     * @param args

     * @Author YHJ create at 2011-11-12 下午07:52:18

     */

    public static void main(String[] args) {

        List<String> cases = new ArrayList<String>();

        while(true){

            cases.add(new String());

        }



    }
}
