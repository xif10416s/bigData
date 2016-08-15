package com.fxi.test.spark.mllib

import com.fxi.test.spark.common.constants.CommonConstants
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
  * Created by xifei on 16-5-18.
  */
case class KeyedPoint(label: String,
                      features: Vector) {
  override def toString: String = {
    s"($label,$features)"
  }
}

object KeyedPoint {
  def parse(s: String): KeyedPoint = {

    val parts = s.split(CommonConstants.COMON)
    val label = parts(0)
    val features = Vectors.dense(parts(1).trim().split(CommonConstants.TAB).map(java.lang.Double.parseDouble))
    new KeyedPoint(label, features)
  }
}
