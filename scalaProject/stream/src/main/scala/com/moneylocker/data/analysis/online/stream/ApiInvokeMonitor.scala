package com.moneylocker.data.analysis.online.stream

import java.util.HashMap

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}


import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
 * Created by xifei on 15-9-24.
 * 122.144.134.67:2181,122.144.134.67:2182,122.144.134.67:2183 kafkaGroup ml_obtain_credit_log4 2
 */
object ApiInvokeMonitor {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(":"))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(1), Seconds(1), 2)
    wordCounts.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        rdd.collect().foreach(
          x => {
            println( x._1 +":"+x._2)
          })
//        val partial = rdd.first()
//        println("Approx distinct users this batch: %s".format(partial._1))
      }
    })
    //    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _ );
    //wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
