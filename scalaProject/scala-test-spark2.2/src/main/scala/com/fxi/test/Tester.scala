package com.fxi.test

import org.apache.spark.sql.SparkSession


/**
  * Created by seki on 17/9/16.
  */
object Tester {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .master("local[*]")
//      .config("spark.jars", "/Users/seki/git/learn/bigData/scalaProject/scala-test-spark2.2/target/scala-test-spark2.2-1.0-SNAPSHOT.jar")
      .getOrCreate()
    val count = spark.sparkContext.parallelize(Seq(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3), 4)
      .groupBy(f => f)
    count.collect().foreach(println _)
  }

}
