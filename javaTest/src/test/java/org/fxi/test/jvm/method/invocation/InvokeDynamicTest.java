package org.fxi.test.jvm.method.invocation;
import static java.lang.invoke.MethodHandles.lookup;
import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Created by seki on 17/4/5.
 * http://www.infoq.com/cn/articles/jdk-dynamically-typed-language
 */
public class InvokeDynamicTest {
    public static void main(String[] args) throws Throwable {
        //1
        INDY_BootstrapMethod().invokeExact("icyfenix");
    }
    public static void testMethod(String s) {
        System.out.println("hello String:" + s);
    }
    public static CallSite BootstrapMethod(MethodHandles.Lookup lookup, String name, MethodType mt) throws Throwable {
        return new ConstantCallSite(lookup.findStatic(InvokeDynamicTest.class, name, mt));
    }
    //描述BootstrapMethod签名的类型,
    private static MethodType MT_BootstrapMethod() {
        return MethodType.fromMethodDescriptorString("(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;)Ljava/lang/invoke/CallSite;", null);
    }
    private static MethodHandle MH_BootstrapMethod() throws Throwable {
        //2 查找InvokeDynamicTest类的静态方法BootstrapMethod,
        return lookup().findStatic(InvokeDynamicTest.class, "BootstrapMethod", MT_BootstrapMethod());
    }
    private static MethodHandle INDY_BootstrapMethod() throws Throwable {
        CallSite cs = (CallSite) MH_BootstrapMethod().invokeWithArguments(lookup(), "testMethod", MethodType.fromMethodDescriptorString("(Ljava/lang/String;)V", null));
        return cs.dynamicInvoker();
    }
}
