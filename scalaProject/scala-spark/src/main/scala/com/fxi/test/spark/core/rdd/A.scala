package com.fxi.test.spark.core.rdd

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xifei on 16-7-12.
  */

case class A(a: String, b: B) extends Tag {
  override def getTag: Array[PropTag] = {
    val a = new ArrayBuffer[PropTag]()
    a ++= b.getTag
    a.toArray
  }
}

case class B(b: String) extends Tag {
  def getB(): String = {
    return b + "11"
  }

  override def getTag: Array[PropTag] = {
    return Array(PropTag("a", "a"), PropTag("b", "b"))
  }
}

case class PropTag(label: String, value: String)

trait Tag {
  def getTag: Array[PropTag]
}