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
}
