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
    df.show()
    df.printSchema()
    val ds = spark.read.json("../../spark/main/resources/people.json").as[Person]
    ds.filter(ds("age")>19).show()
    println(ds.schema)
    spark.stop()
  }

}

