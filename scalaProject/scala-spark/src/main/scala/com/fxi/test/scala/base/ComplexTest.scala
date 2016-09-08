package com.fxi.test.scala.base

import com.fxi.test.scala.base.bean.Operator
import org.junit.Test

/**
  * Created by xifei on 16-8-30.
  */
class ComplexTest {
  @Test
  def testInvokeResultHandlerCall() = {
    def parse[Int](command: String)(toResult: String => Int): Int = {
      //pre handler common
      println(command)
      //dynamic handle result when call impl
      toResult(command)
    }

    val rs = parse("aaa")(parser => {
      parser.hashCode
    })

    println(rs)
  }

  @Test
  def testCaseCall() = {

    val rs = new Operator match {
      case a: Operator =>
        a transformExpressionsUp {
          // 方法的参数是一个函数体
          //          do some check
//          case b: String =>
//            println(b)
//          case _ => {
//          }
            if(a != null){

            }

            println("00")
            "123"
        }
    }

    println(rs)

  }
}
