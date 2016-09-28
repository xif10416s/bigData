package com.fxi.test.spark.sql.datasets

import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.math._

/**
  * Created by xifei on 16-8-15.
  */
class BaseDatasetsTaster {
  @Test
  def test01() = {
    val spark = SparkSession
    .builder
    .appName("Spark SQL Example")
    .master("local[*]")
    .getOrCreate()
    import spark.implicits._

    val df = spark.read.json("../../spark/main/resources/people.json")
    df.createOrReplaceTempView("people")
//    val query = spark.sql("select * from  (select * from people  where name is not null limit 10) p1 join people p2 on p1.name = p2.name where p1.age > 19 limit 100 ")
    val query = spark.sql("select * from people where age > 19 limit 100 ")

    query.collect()
    println("+++++++++++++++++++logical plan ++++++++++++++++++")
    println(query.queryExecution.logical)
    println("+++++++++++++++++++analyzed plan ++++++++++++++++++")
    println( query.queryExecution.analyzed)
    println("+++++++++++++++++++optimizedPlan plan ++++++++++++++++++")
    println( query.queryExecution.optimizedPlan)
    println("+++++++++++++++++++executedPlan plan ++++++++++++++++++")
    println( query.queryExecution.executedPlan)
    //    df.show()
//    df.printSchema()
//    val ds = spark.read.json("../../spark/main/resources/people.json").as[Person]
//    ds.filter($"age">19).show()
//    println(ds.schema)
    spark.stop()
  }

}

