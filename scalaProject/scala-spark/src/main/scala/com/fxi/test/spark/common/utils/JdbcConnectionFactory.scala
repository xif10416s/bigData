package com.fxi.test.spark.common.utils

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD.ConnectionFactory

/**
 * Created by xifei on 15-10-9.
 */

object JdbcConnectionFactory {
  val testUrl = "jdbc:mysql://122.144.134.82:8067/hsp_clog"
  def getConnectionFactory( url:String , user:String , pass:String):ConnectionFactory = {
    new ConnectionFactory {
      @throws[Exception]
      override def getConnection: Connection = {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        DriverManager.getConnection(url, user, pass)
      }
    }

  }
}
