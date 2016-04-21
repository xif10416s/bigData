package com.fxi.test.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xifei on 16-4-21.
  */
object LeftJoinSqlTest {
  def main(args: Array[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    val logLevel = Level.ERROR
    Logger.getLogger("org").setLevel(logLevel)

    val Array(master) = Array("local[*]");
    val sparkConf = new SparkConf().setAppName("LeftJoinSqlTest").setMaster(master)
    val sc = new SparkContext(sparkConf)
    val sql = new SQLContext(sc)
    val a1 = new ArrayBuffer[UserAdBean]()
    a1+= UserAdBean("1","1",1)
    a1+= UserAdBean("2","2",2)
    a1+= UserAdBean("3","3",3)

    val a2 = new ArrayBuffer[UserAdBean]()
    a2+= UserAdBean("1","1",1)
    a2+= UserAdBean("2","2",2)
    a2+= UserAdBean("4","4",4)

    sql.createDataFrame(sc.makeRDD(a1)).registerTempTable("t1")
    sql.createDataFrame(sc.makeRDD(a2)).registerTempTable("t2")

    sql.sql("select t1.userId , t1.adId, t1.cnt , t2.userId, t2.adId , t2.cnt from t1 t1 left join t2 t2 on t1.userId= t2.userId and t1.adId = t2.adId where t2.userId is null ").show(10)
  }
}

case class UserAdBean(userId:String , adId :String , cnt : Long)
