package com.fxi.test.spark.core.rdd

import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.math._

/**
  * Created by xifei on 16-8-15.
  */
object BaseTaster {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .config("spark.jars","scalaProject/testSpark2-0/target/scala-test-spark2.0-1.0-SNAPSHOT.jar")
      .master("spark://192.168.70.176:61070")
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
