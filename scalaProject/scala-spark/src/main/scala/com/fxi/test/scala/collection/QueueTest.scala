package com.fxi.test.scala.collection

import org.apache.spark.rdd.RDD
import org.junit.Test

import scala.collection.mutable.{Queue, ArrayBuffer}

/**
  * Created by xifei on 16-4-8.
  */
class QueueTest {
  @Test
  def test(): Unit = {
    println("aaa")
  }

  @Test
  def testPlus(): Unit = {
    println(initQueue())
  }

  @Test
  def testDequeue(): Unit = {
    val buffer = new ArrayBuffer[Int]()
    val queue = initQueue();
    buffer += queue.dequeue()
    println(buffer.head)
    Some(buffer.head)
  }

  def initQueue(): Queue[Int] = {
    val rddQueue = new Queue[Int]()
    for (i <- 1 to 100) {
      rddQueue.synchronized {
        rddQueue += i
      }
    }
    rddQueue
  }
}
