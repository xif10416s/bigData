package org.fxi.test.jvm.memory.exception;

import junit.framework.TestCase;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * Created by seki on 17/4/7.
 * 方法区溢出测试
 * -XX:MaxMetaspaceSize=2M
 */
public class MethodAreaOutOfMemory {
    public static void main(String[] args) {

        while(true){

            Enhancer enhancer = new Enhancer();

            enhancer.setSuperclass(TestCase.class);

            enhancer.setUseCache(false);

            enhancer.setCallback(new MethodInterceptor() {

                @Override

                public Object intercept(Object arg0, Method arg1, Object[] arg2,

                                        MethodProxy arg3) throws Throwable {

                    return arg3.invokeSuper(arg0, arg2);

                }

            });

            enhancer.create();

        }

    }
}
