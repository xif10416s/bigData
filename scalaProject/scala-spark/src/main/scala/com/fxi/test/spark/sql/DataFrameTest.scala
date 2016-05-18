package com.fxi.test.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
/**
  * Created by xifei on 16-5-5.
  */
object DataFrameTest {
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level}
    val Array(master) = Array("local[*]");
    val sparkConf = new SparkConf().setAppName("DataFrameTest").setMaster(master)
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel(Level.ERROR.toString)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("./spark/main/resources/people.json")

    df.show()

    df.filter("age > 20").show()

    println(df.groupBy("age").count().orderBy(desc("count")).show())

    sqlContext.sql("SELECT * FROM json.`./spark/main/resources/people.json`").show()
  }
}
