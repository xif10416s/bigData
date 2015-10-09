package com.moneylocker.data.analysis.util

import java.sql.ResultSet

import org.apache.spark.api.java.function.Function
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}


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


}


// cassandraTableSchema

