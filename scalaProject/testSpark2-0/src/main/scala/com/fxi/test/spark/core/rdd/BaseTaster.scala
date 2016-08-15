package com.fxi.test.spark.core.rdd

import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.math._

/**
  * Created by xifei on 16-8-15.
  */
class BaseTaster {
  @Test
  def test01() = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
    spark.stop()
  }

}
