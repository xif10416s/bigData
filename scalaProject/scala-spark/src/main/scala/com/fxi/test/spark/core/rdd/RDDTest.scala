package com.fxi.test.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by seki on 16/4/23.
  */
class RDDTest {
  val sparkConf = new SparkConf().setAppName("RDDTest").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  @Test
  def testCount(): Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(1 to 1000, 10)
    println(rdd1.count())
  }

  @Test
  def testForEachPartition(): Unit = {
    val rs = sc.makeRDD(1 to 1000, 10).mapPartitions(f => {
      f.map[Int](i => {
        1000 + i
      }).filter(f => f % 2 == 0)
    })

    println(rs.collect().mkString(","))
  }

  @Test
  def testFileToRDD(): Unit = {
    val userArray = for (line <- scala.io.Source.fromFile("./data/test/userAndAd").getLines()) yield line
    sc.parallelize(userArray.toSeq).foreach(println(_))

  }
}
