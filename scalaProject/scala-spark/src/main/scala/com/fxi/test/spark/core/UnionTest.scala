package com.fxi.test.spark.core

import com.fxi.test.spark.common.TestBean
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.Test

/**
  * Created by xifei on 16-4-13.
  * rdd union
  * sc union
  */
class UnionTest {
  val Array(master) = Array("local[*]");
  val sparkConf = new SparkConf().setAppName("UnionTest").setMaster(master)
  val sc = new SparkContext(sparkConf)
  @Test
  def test(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val logLevel = Level.ERROR
    Logger.getLogger("org").setLevel(logLevel)

//    sc.setCheckpointDir("./scalaProject/checkpoint/")
    var rdd:RDD[Int] = null
    val rdd1 =  sc.makeRDD(1 to 100000000, 10)
    val rdd2 =  sc.makeRDD(1 to 100000000, 10)
    val rdd3 =  sc.makeRDD(1 to 100000000, 10)
    val rdd4 =  sc.makeRDD(1 to 100000000, 10)

    val rddArr = Array(rdd1,rdd2,rdd3,rdd4)

    val scUnionRdd = sc.union(rddArr)
    println("sc==>"+scUnionRdd.count())


    rdd = rdd1.union(rdd2)
    rdd = rdd.union(rdd3)
    rdd = rdd.union(rdd4)

    rdd1.filter(f =>{
      f % 2==0
    }).cache().count()

    rdd2.filter(f =>{
      f % 2==0
    }).checkpoint()

    println("rdd==>"+rdd.count())
    rdd.checkpoint()

    println("sc.union(rddArr) ==>" + sc.union(rddArr).count())

    Thread.sleep(1000000);
  }


  @Test
  def testUnion(): Unit = {
    val a = Array(("1", TestBean(1L,"a")),("2", TestBean(1L,"b")),("3", TestBean(1L,"c")))
    val b = Array(("1", TestBean(1L,"d")),("4", TestBean(1L,"e")),("5", TestBean(1L,"f")))

    val c = sc.makeRDD(a).union(sc.makeRDD(b))
    c.reduceByKey( (f1,f2) =>{
      f1
    }).foreach(println _)
  }
}

