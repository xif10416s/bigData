package com.fxi.test.spark.stream

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.alibaba.fastjson.JSON
import com.fxi.test.spark.common.utils.FileUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, SQLContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


/**
 * Created by xifei on 15-9-24.
 * 192.168.0.248:2181,192.168.0.248:2182,192.168.0.248:2183 kafkaGroup ml_obtain_credit_log4 2 2 10 10 jdbc:mysql://122.144.134.67:3306/servicedb huaqianv3 huaqianv3 local[*] 10g 2
 * ./start_ad_action_monitor.sh 122.144.134.67:2181,122.144.134.67:2182,122.144.134.67:2183 kafkaGroup ml_obtain_credit_log4 2 10 60 60 jdbc:mysql://10.144.134.67:3306/servicedb huaqianv3 huaqianv3 spark://10.144.134.67:7077 1g 4
 *
 * ./start_ad_action_monitor.sh 10.144.134.92:12181,10.144.134.93:12181,10.144.134.94:12181 sparkStreamAdAction2 ml_ad_left_slide_log 8 10 120 120 jdbc:mysql://10.144.134.70:3306/hqlog log1 log1_huaqian spark://10.144.134.82:7077 1g 2
 * #./start_ad_action_monitor_exposure.sh 10.144.134.92:12181,10.144.134.93:12181,10.144.134.94:12181 sparkStreamAdAction2 ml_ad_exposure_log 32 10 120 120 jdbc:mysql://10.144.134.70:3306/hqlog log1 log1_huaqian spark://10.144.134.67:7077 4g 8
 *
 * ./start_ad_action_monitor_exposure.sh 10.144.134.92:12181,10.144.134.93:12181,10.144.134.94:12181 sparkStreamAdAction2 ml_ad_exposure_log 32 2 120 120 jdbc:mysql://10.144.134.70:3306/hqlog log1 log1_huaqian spark://10.144.134.82:7077 4g 8
 */
object KafkaSteam {
  //val url = "jdbc:mysql://10.144.134.67:3306/servicedb"
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads> <batchDuration> <windowLength> <slidingInterval> <memory> <core> ")
      System.exit(1)
    }
    import org.apache.log4j.{Level, Logger}
    val logLevel = Level.ERROR
    Logger.getLogger("org").setLevel(logLevel)

    val Array(zkQuorum, group, topics, numThreads ,batchDuration ,windowLength,slidingInterval ,url,user,password ,master,memorySize , coreSize) = args
    val sparkConf = new SparkConf().setAppName("AdActionMonitor ==>" + topics).setMaster(master)
    //sparkConf.set("spark.driver.host","10.144.134.82")
    sparkConf.set("spark.cleaner.ttl","600")
    sparkConf.set("spark.executor.memory",memorySize)
    sparkConf.set("spark.executor.cores",coreSize)
    println("coreSize" + coreSize)
    val ssc = new StreamingContext(sparkConf, Seconds(batchDuration.toInt))

    //ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val datas = lines.map(
      f => {
        println(f)
        JSON.parseObject(f).getString("adId") + ":" + JSON.parseObject(f).getString("actionType")
      }
    ).map(x => (x, 1L)).reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Seconds(windowLength.toInt), Seconds(slidingInterval.toInt)).map[AdTypeBean](
        f => {
            val mergeId = f._1.split(":")
            val cal =  Calendar.getInstance();
            new AdTypeBean(mergeId(0),mergeId(1).toShort,FileUtils.toDayInt(cal),new SimpleDateFormat("HH:mm").format(cal.getTime),f._2.toLong)
        }
      ).foreachRDD(rdd=>{
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        val p = new Properties()
        p.put("user",user)
        p.put("password",password)
        try{
          sqlContext.createDataFrame(rdd).write.mode(SaveMode.Append).jdbc(url ,"ml_ad_real_time_log", p)
        } catch {
          case e :Exception =>{
            e.printStackTrace()
          }
        }

    })

    ssc.start()
    ssc.awaitTermination()
  }
}

case class AdTypeBean(ad_id: String, action_type: Short ,day :Int,time:String,cnt:Long)
