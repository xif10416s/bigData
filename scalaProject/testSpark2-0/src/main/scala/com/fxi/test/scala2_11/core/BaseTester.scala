package com.fxi.test.scala2_11.core

/**
  * Created by xifei on 16-8-18.
  */
class BaseTester {

}

object BaseTester {
  def main(args: Array[String]) {
    val a = "11"
    val rs =s"""
       |${a}
       |${doProduce(s"${a}")}
     """.stripMargin

    println(rs)
  }

  def doProduce(a:String):String = {
    "123"
  }
}
