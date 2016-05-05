package com.fxi.test.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import spire.implicits

/**
  * Created by xifei on 16-5-5.
  * using functional transformations (map, flatMap, filter, etc.).
  * 这些操作不用反序列化就可以操作
  * instead of using Java Serialization or Kryo they use a specialized Encoder to serialize the objects
  * for processing or transmitting over the network
  */
object DatasetTest {
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level}
    val Array(master) = Array("local[*]");
    val sparkConf = new SparkConf().setAppName("DatasetTest").setMaster(master)
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)
    val sqlContext = new SQLContext(sc)


    import sqlContext.implicits._
    val ds = Seq(1, 2, 3).toDS()
    ds.map(_ + 1).show() // Returns: Array(2, 3, 4)


    val ds2 = Seq(Person("Andy", 32)).toDS()
    ds2.filter(f => f.age > 20).show()

    sqlContext.read.json("./spark/main/resources/people.json").filter("age != null").as[Person].show()
  }
}

case class Person(name: String, age: Long)