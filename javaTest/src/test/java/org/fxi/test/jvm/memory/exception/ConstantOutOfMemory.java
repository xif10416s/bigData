package org.fxi.test.jvm.memory.exception;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by seki on 17/4/7.
 * 常量池内存溢出探究
 *  args : -XX:MaxMetaspaceSize=2M
 */
public class ConstantOutOfMemory {
    /**

     * @param args

     * @throws Exception


     */

    public static void main(String[] args) throws Exception {

        try {

			List<String> strings = new ArrayList<String>();

            int i = 0;

            while(true){

                strings.add(String.valueOf(i++).intern());

            }

        } catch (Exception e) {

            e.printStackTrace();

            throw e;

        }



    }
}
