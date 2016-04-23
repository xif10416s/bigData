package com.fxi.test.scala.base

import org.junit.Test

/**
  * Created by xifei on 16-4-22.
  */
class BaseTest {
  @Test
  def testBaseOperation(): Unit = {
    // A opt B  == (A).opt(B)
    require((3).+(4) == 3 + 4)
    println("a" toString())
    println("abc" substring (1) equals "abc".substring(1))

    def g() {
      "this String gets lost too"
    }
    g()
  }

  @Test
  def testMvOperation(): Unit = {
    println(1 << 2)
  }

  /**
    * The implicit
    * modifier in front of the method tells the compiler to apply it automatically in
    * a number of situations.
    */
  @Test
  def testImplicitConversions(): Unit = {

    implicit def stringToInt(s: String): Int = s.hashCode
    println("aa" / 10)
  }

  /**
    * for clauses yield body
    * The yield goes before the entire body
    */
  @Test
  def testYield(): Unit = {
    val list = Array(1, 2, 3, 4, 5)
    def oddList = for {l <- list if l % 2 == 0} yield l
    oddList.foreach(f => println(f))
  }

  /**
    * (x: Int) => x + 1
    * The => designates that this function converts the thing on the left (any integer
    * x ) to the thing on the right ( x + 1 ).
    * 函数映射
    */
  @Test
  def testFirstClass(): Unit = {
    //定义了一个函数,从 x 转换到 x+1
    val increase = (x: Int) => x + 1
    println(increase(19))

    val someNumbers = List(-11, -10, -5, 0, 5, 10)
    someNumbers.filter((x: Int) => x > 0).foreach(f => println(f))
    someNumbers.filter(_ > 0).foreach(f => println(f)) //Placeholder syntax
    val f = (_: Int) + (_: Int)
    f(5, 19)
    someNumbers.foreach(println _) //someNumbers.foreach(x => println(x))

  }

  /**
    * The function value (the object) that’s created at runtime from this function
    * literal is called a closure.
    */
  @Test
  def testClosure(): Unit = {
    def makeIncreaser(more: Int) = (x: Int) => x + more
    val inc1 = makeIncreaser(1)
    val inc9999 = makeIncreaser(9999)
    inc1(10)
    inc9999(10)
  }

  /**
    * curring ==> 函数柯理化
    * 变成两个函数相继调用
    */
  @Test
  def testCurrying() = {
    def plainOldSum(x: Int, y: Int) = x + y
    println(plainOldSum(1, 3))

    def curriedSum(x: Int)(y: Int) = x + y
    println(curriedSum(1)(3))

    //===>定义了一个函数first 函数体又是一个函数
    def first(x: Int) = (y: Int) => x + y
    println(first(1)(3)) //相当于调用两次函数,第一次first(1) 返回一个函数 在调用 (3)
  //单个括号的时候可以用 花括号
    println(first {
      1
    } {
      3
    })

    val second = curriedSum(1) _;
    println(second(3))

    //定义一个函数twice,参数是一个叫op 输入输出都是double的函数,和一个x的double, 函数体是 执行两次op操作
    def twice(op: Double => Double, x: Double) = op(op(x))
    println(twice(_ + 1, 5))
  }

  /**
    * control abstractions
    */
  @Test
  def testControlAbstraction() = {
    //一个参数的时候可以用 花括号 代替 括号
    println("Hello, world!")
    println {
      "Hello, world!"
    }
  }

  /**
    * 空参数列表
    * => 函数体
    */
  @Test
  def testByNameParameter() = {
    var assertionsEnabled = true
    def myAssert(predicate: () => Boolean) =
      if (assertionsEnabled && !predicate())
        throw new AssertionError
//    myAssert(()=>3>5)

    //==> “() => Boolean”   --> “=> Boolean”

    def byNameAssert(predicate: => Boolean) =
      if (assertionsEnabled && !predicate)
        throw new AssertionError

    // 和 直接定义参数区别
    // predicate: Boolean <== 定义一个变量, 调用方法前已经计算
    // predicate: => Boolean  <== 定义了一个函数,没有参数,计算的时候才计算
    def boolAssert(predicate: Boolean) =
      if (assertionsEnabled && !predicate)
        throw new AssertionError

    byNameAssert(10/0 ==0)
    boolAssert(10/10 == 0)
    println("")
  }

  @Test
  def testImplicitParam() = {
    implicit val s = "aaa";
    def f(a:Int)(implicit s:String): Unit = {
      println(s)
      println(a)
    }

    f(1)
  }

}
