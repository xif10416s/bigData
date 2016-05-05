package com.fxi.test.spark.common.utils

import java.util

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.{CassandraRow, ColumnName, SparkContextFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer


/**
 * Created by xifei on 15-9-29.
 *
 */
object CassandraUtil {


  def formatActionDayKeyParam(days: Long*): String = {
    val sb = new StringBuffer();
    for (day <- days) {
      for (i <- 0 to 255) {
        sb.append(day * 1000L + i).append(",");
      }
    }
    val query = sb.toString();
    query.substring(0, query.length() - 1);
  }

  def queryCassandra(cc: org.apache.spark.sql.cassandra.CassandraSQLContext, day: Long, sql: String): RDD[Row] = {
    println(sql.format(day * 1000L + 0))
    var unionDf = cc.sql(sql.format(day * 1000L + 0)).rdd
    for (i <- 1 to 255) {
      unionDf = unionDf.union(cc.sql(sql.format(day * 1000L + i)).rdd)
    }
    unionDf
  }

  def initQuerySql(): String = {
    val sb = new StringBuffer();
    for (i <- 0 to 255) {
      sb.append("?").append(",");
    }
    val query = sb.toString();
    query.substring(0, query.length() - 1);
  }

  def initParams(day: Long)(): Array[Long] = {
    val p = new Array[Long](256)
    for (i <- 0 to 255) {
      p(i) = day * 1000 + i;
    }
    p;
  }


  def noCreditLeftAction(cc: org.apache.spark.sql.cassandra.CassandraSQLContext, days: Long*): Unit = {
    for (day <- days) {
      val shareDf = cc.sql("select distinct user_id , ad_id from mlapp.ml_ad_share_log where action_day_key in(%s)".format(CassandraUtil.formatActionDayKeyParam(day)))
      //leftDf.registerTempTable("ml_ad_left_slide_log")
      val creditDf = cc.sql("select distinct user_id , ad_id from mlapp.ml_obtain_credit_log where action_day_key in(%s)".format(CassandraUtil.formatActionDayKeyParam(day)))
      creditDf.cache();

      val browseDf = cc.sql("select distinct user_id , ad_id from mlapp.ml_ad_browse_log where action_day_key in(%s)".format(CassandraUtil.formatActionDayKeyParam(day)))

      println(" mlapp.ml_ad_browse_log ...............")
      //creditDf.registerTempTable("ml_obtain_credit_log")
      val sub = shareDf.rdd.subtract(creditDf.rdd)
      val path = "/tmp/ml/noCreditLeftAction/" + day + ".txt";
      FileUtils.saveToFile(path, sub.collect())

      FileUtils.saveToFile(path, browseDf.rdd.subtract(creditDf.rdd).collect())
    }
  }

  def queryCassandra(day:Long  , scf:SparkContextFunctions ,keySpace:String ,table:String ,fileds:Array[String]):RDD[CassandraRow] ={
    val rddArray = ArrayBuffer[CassandraRDD[CassandraRow]]()
    val filedsCol = new Array[ColumnName](fileds.length)
    for (i <- 0 to fileds.length - 1) {
      filedsCol(i) = new ColumnName(fileds(i), Option.empty[String])
    }
    val whereSql = "action_day_key = ? ";
    for (i <- 0 to 255) {
      rddArray +=scf.cassandraTable(keySpace, table).where(whereSql, day * 1000L + i).select(filedsCol: _*);
    }

    scf.sc.union(rddArray)
  }

  def queryCassandra(days:Array[Long]  , scf:SparkContextFunctions ,keySpace:String ,table:String ,fileds:Array[String]):RDD[CassandraRow] ={
    val rddArray = ArrayBuffer[RDD[CassandraRow]]()
    for(day <- days ){
      rddArray += queryCassandra(day,scf,keySpace,table,fileds)
    }
    scf.sc.union(rddArray)
  }

  def queryCassandraWithConnect(day:Long  , scf:SparkContextFunctions ,keySpace:String ,table:String ,fileds:Array[String],cassandraConnector: CassandraConnector):RDD[CassandraRow] ={
    val rddList =  new util.ArrayList[CassandraRDD[CassandraRow]]()
    val filedsCol = new  Array[ColumnName](fileds.length)
    for(i <- 0 to fileds.length -1 ){
      filedsCol(i) = new ColumnName(fileds(i), Option.empty[String])
    }
    val whereSql = "action_day_key = ? ";
    for(i <- 0 to 255){
      rddList.add(scf.cassandraTable(keySpace,table).where(whereSql,day*1000L+i).select(filedsCol:_*).withConnector(cassandraConnector))
    }
    var unionRdd:RDD[CassandraRow] = rddList.get(0)
    for(i <- 1 to 255){
      unionRdd = unionRdd.union(rddList.get(i))
    }
    unionRdd
  }


}


// cassandraTableSchema

