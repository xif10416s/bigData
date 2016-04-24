package com.fxi.test.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test

/**
  * Created by seki on 16/4/23.
  */
class RDDTest {
  val sparkConf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  @Test
  def testCount(): Unit = {
    val rdd1 : RDD[Int] =  sc.makeRDD(1 to 1000, 10)
    println(rdd1.count())
  }
}
