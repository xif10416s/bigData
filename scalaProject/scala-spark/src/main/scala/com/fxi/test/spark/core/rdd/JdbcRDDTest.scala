package com.fxi.test.spark.core.rdd

import java.sql.ResultSet
import java.util.Properties

import com.fxi.test.spark.common.utils.JdbcConnectionFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by xifei on 16-4-18.
  */
object JdbcRDDTest {
  val url = "jdbc:mysql://122.144.134.67:3306/servicedb"
  val user = "huaqianv3"
  val pass = "huaqianv3"

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    val logLevel = Level.ERROR

    Logger.getLogger("org").setLevel(logLevel)
    val sparkConf = new SparkConf().setAppName("UnionTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val cnt = new JdbcRDD[Long](sc, () => JdbcConnectionFactory.getConnectionFactory(url, user, pass).getConnection,
      "select id  from hq_allocate_ad_log  WHERE id >= ? AND id <= ?", 1460938817747001101L, 1460969380500000101L, 10, (r: ResultSet) => {
        r.getLong("id")
      }).count()

    val sql: SQLContext = new SQLContext(sc)
    val p = new Properties()

    sql.read.jdbc(url,"hq_allocate_ad_log",Array("id > 0"),p)


    println("=======>"+cnt)
  }
}
