package com.fxi.test.spark.thirdpart.cassandra

import com.datastax.spark.connector.{RDDFunctions, SparkContextFunctions}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xifei on 16-4-22.
  */
object CassandraConnectTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("CassandraConnectTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf);

    val cc = new org.apache.spark.sql.cassandra.CassandraSQLContext(sc);
    val scf = new SparkContextFunctions(sc)
    val sqlContext = new SQLContext(sc)

    val transformRdd = scf.cassandraTable("ml_statistics", "ml_ad_exp_allocate_download_log").map[ExpoureAllocateDownloadLog](f => {
      new ExpoureAllocateDownloadLog(f.getLong("action_day_key"), f.getString("userid"), f.getString("adid"), f.getLong("expcnt"), f.getLong("allocatecnt"), f.getLong("isdownload"), 1L)
    })
    val rddf = new RDDFunctions(transformRdd)
    rddf.saveToCassandra("ml_statistics", "ml_ad_exposure_allocate_download_log")

  }
}

case class ExpoureAllocateDownloadLog(action_day_key:Long , userid:String ,adid:String ,expcnt:Long ,allocatecnt:Long ,isdownload:Long , adtype:Long)
