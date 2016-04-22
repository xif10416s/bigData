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
    println("abc" substring(1) equals "abc".substring(1))

    def g() { "this String gets lost too" }
    g()
  }

  @Test
  def testMvOperation(): Unit = {
    println(1 << 2)
  }
}
