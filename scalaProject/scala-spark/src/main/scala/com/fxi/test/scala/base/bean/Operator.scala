package com.fxi.test.scala.base.bean

/**
  * Created by xifei on 16-8-30.
  */
class Operator {
  def transformExpressionsUp(rule:String):Int = {
    //do something
    println(rule)
    rule.hashCode
  }
}
