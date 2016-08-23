package com.fxi.test.scala.base.reflection

import scala.reflect.api.JavaUniverse

/**
  * Created by xifei on 16-8-23.
  */
object TypesTest {
  def main(args: Array[String]) {
    import scala.reflect.runtime.universe._
    val unit: JavaUniverse#Type = typeOf[List[Int]]
    println(unit)
  }
}
