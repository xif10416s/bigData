package com.moneylocker.data.analysis.util

import java.sql.ResultSet

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by xifei on 15-10-9.
 *
 * import com.moneylocker.data.analysis.util._
 * import org.apache.spark.rdd.JdbcRDD
 * import java.sql.ResultSet
 *
 * import org.apache.log4j.{Level, Logger}
 * val logLevel = Level.WARN
 * Logger.getLogger("org").setLevel(logLevel)
 *
 *
 * /data/cluster/spark/1.3.1/spark-1.3.1-bin-hadoop2.6/bin/spark-shell --master local[4] --name test --conf spark.cassandra.connection.host=122.144.134.82  --jars /home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/stream-1.0-SNAPSHOT.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/spark-cassandra-connector-java_2.10-1.3.0-M1.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/spark-cassandra-connector_2.10-1.3.0-M1.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/cassandra-driver-core-2.1.6.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/guava-14.0.1.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/cassandra-thrift-2.1.3.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/joda-time-2.3.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/spark-csv_2.10-1.2.0.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/commons-csv-1.1.jar,/home/xifei/IdeaSpacke/ml-data-analysis-online-bigdata/stream/target/streamlib/lib/mysql-connector-java-5.1.34.jar
 *
 * /was/spark/bin/spark-shell --master spark://10.144.134.91:7077 --name test --conf spark.cassandra.connection.host=122.144.134.82  --jars /home/twas3/stream-1.0-SNAPSHOT/lib/stream-1.0-SNAPSHOT.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/spark-cassandra-connector-java_2.10-1.3.0-M1.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/spark-cassandra-connector_2.10-1.3.0-M1.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/cassandra-driver-core-2.1.6.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/guava-14.0.1.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/cassandra-thrift-2.1.3.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/joda-time-2.3.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/spark-csv_2.10-1.2.0.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/commons-csv-1.1.jar,/home/twas3/stream-1.0-SNAPSHOT/lib/mysql-connector-java-5.1.34.jar

 */
object Tester {

  def main(args: Array[String]) {
    val conf = new SparkConf();
    conf.setAppName("Link Spark and Cassandra");
    conf.setMaster("local[*]");
    conf.set("spark.cassandra.connection.host", "122.144.134.82");


    val sc = new SparkContext(conf);


    val cc = new org.apache.spark.sql.cassandra.CassandraSQLContext(sc);

    val df = cc.sql("select user_id , action_day_key , obtain_time from mlapp.ml_obtain_credit_log where action_day_key in(%s)".format(com.moneylocker.data.analysis.util.CassandraUtil.formatActionDayKeyParam(20150919)))
    //println(df.printSchema());
    println(df.select("user_id").first());
    df.registerTempTable("ml_obtain_credit_log")
    // println(df.printSchema());
    val f = cc.sql("select user_id from ml_obtain_credit_log")
    println(f.first());




    val userInfoRdd = new JdbcRDD[UserInfo](sc, () => JdbcConnectionFactory.getConnectionFactory(JdbcConnectionFactory.testUrl, "test", "test").getConnection,
      "select id , app_version from hq_user_info  WHERE state >= ? AND state <= ?", 1, 150, 2, (r: ResultSet) => {
        new UserInfo(r.getString(1), r.getInt(2))
      })

    cc.createDataFrame(userInfoRdd).registerTempTable("user_info")

    val u = cc.sql("select * from user_info")
    println(u.first());



    val m =  cc.sql("select u.id from user_info u , ml_obtain_credit_log c where u.id = c.user_id");
    println(m.first());

    val verifyLogDf = cc.sql("select send_time, verify_type from mlapp.ml_verify_log");
    verifyLogDf.registerTempTable("ml_verify_log");

    cc.sql("select send_time from ml_verify_log where verify_type = 1 and send_time >='2015-09-01' and send_time <'2015-10-01'  order by send_time").selectExpr("SUBSTRING(send_time ,0 ,10) as day").groupBy("day").count().show();


    //m.save("/tmp/ml/newcars", "com.databricks.spark.csv")
  }
}

case class UserInfo(id: String, app_version: Int)



