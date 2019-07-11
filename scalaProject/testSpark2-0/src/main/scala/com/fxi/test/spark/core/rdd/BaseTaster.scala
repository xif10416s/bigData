package com.fxi.test.spark.core.rdd

import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.math._

/**
  * Created by xifei on 16-8-15.
  */
object BaseTaster {


  def main(args: Array[String]): Unit = {
    test03()
  }

  def test01() ={
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val groupRdd = spark.sparkContext.parallelize(1 until n, slices)
        .groupBy(f => f)
    groupRdd.collect()
      .foreach(println _)
    spark.stop()
  }


  def test02() ={
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
      .getOrCreate()
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val groupRdd = spark.sparkContext.parallelize(1 until n, slices)
        .groupBy(f => f)
        .map(_._1)
    groupRdd.collect()
      .foreach(println _)
    spark.stop()
  }


  def test03() ={
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("spark://xifeideMacBook-Pro.local:7077")
      .config("spark.jars", "/Users/seki/git/learn/spark/examples/target/original-spark-examples_2.11-2.2.1-SNAPSHOT.jar")
      .getOrCreate()
    val count = spark.sparkContext.parallelize(Seq(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3), 4)
      .groupBy(f => f)
    count.collect().foreach(println _)
  }

}
