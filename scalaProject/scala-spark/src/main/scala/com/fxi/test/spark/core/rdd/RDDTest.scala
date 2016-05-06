package com.fxi.test.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

/**
  * Created by seki on 16/4/23.
  */
class RDDTest {
  val master ="local[*]"
//  val master ="spark://xifei-HP-Z228-Microtower-Workstation:7077"
  val sparkConf = new SparkConf().setAppName("RDDTest").setMaster(master)
  val sc = new SparkContext(sparkConf)

  @Test
  def testCount(): Unit = {
    val rdd1: RDD[Int] = sc.makeRDD(1 to 1000, 10)
    println(rdd1.count())
  }

  @Test
  def testMapPartition(): Unit = {
    val rs = sc.makeRDD(1 to 1000, 10).mapPartitions(f => {
      f.map[Int](i => {
        1000 + i
      }).filter(f => f % 2 == 0)
    })

    println(rs.collect().mkString(","))
  }

  @Test
  def testForEachPartition(): Unit = {
    sc.makeRDD(1 to 1000, 10).foreachPartition(
      f =>{
        f.foreach( item =>{
          println(item)
        })
      }
    )

  }

  @Test
  def testFileToRDD(): Unit = {
    val userArray = for (line <- scala.io.Source.fromFile("./data/test/userAndAd").getLines()) yield line
    sc.parallelize(userArray.toSeq).foreach(println(_))

  }

  @Test
  def testCache(): Unit = {
    val df = sc.textFile("./data/test/userAndAd").map[String]( f =>{
      println(f)
      f
    })

    val df2 = sc.textFile("./data/test/userAndAd").map[String]( f =>{
      println("2 --"+f)
      f
    })

    val d = df.intersection(df2)

    println(d.count())
    println("------------------")
    println(d.count())
    println("==================")
    d.cache() // 不用赋值
//    println(df.countByValue())
//    println("------------------")
//    println(df.countByValue())
    println("------------------")
    d.repartition(10).foreachPartition( f=>{
      f.foreach(i =>{
        println(s"c - >$i ")
      })
    })
    println(d.collect())
  }

  @Test
  def testZip(): Unit = {
    val a  = sc.makeRDD(Array("a","b","c"))
    val b  = sc.makeRDD(Array("1","2","3"))
    a.zip(b).foreach(println _)
  }
}
