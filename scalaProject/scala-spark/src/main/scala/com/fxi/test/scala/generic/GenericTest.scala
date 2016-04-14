package com.fxi.test.scala.generic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.junit.Test

import scala.reflect.ClassTag

/**
  * Created by xifei on 16-4-14.
  */
class GenericTest {
  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    println("1---runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U]******")
    println(func(rdd.toLocalIterator))
    runJob(rdd, func, 0 until rdd.partitions.length)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T],func: Iterator[T] => U,partitions: Seq[Int]): Array[U] = {
    println("2----runJob[T, U: ClassTag](rdd: RDD[T],func: Iterator[T] => U,partitions: Seq[Int]): Array[U]")
    val cleanedFunc = clean(func)
    println(cleanedFunc(rdd.toLocalIterator))
    runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions) //TODO
  }

  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              func: (TaskContext, Iterator[T]) => U,
                              partitions: Seq[Int]): Array[U] = {
    val results = new Array[U](partitions.size)
    println("3----")
    println(func(null , rdd.toLocalIterator))
    runJob[T, U](rdd, func, partitions, (index, res) => results(index) = res)
    results
  }

  def runJob[T, U: ClassTag](
                              rdd: RDD[T],
                              func: (TaskContext, Iterator[T]) => U,
                              partitions: Seq[Int],
                              resultHandler: (Int, U) => Unit): Unit = {

  }

  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  }

   def clean[F](f: F, checkSerializable: Boolean = true): F = {
    println("clean.........")
    f
  }

  @Test
  def mockCount(): Unit = {
    val sparkConf = new SparkConf().setAppName("UnionTest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val emptyRDD = sc.makeRDD(1 to 100000000, 10)
    runJob( emptyRDD, getIteratorSize _)
  }

  @Test
  def testType() :Unit ={
    println(testT((s:String, b:Int) => "aa".toString))
  }

  def testT[U](func : (String , Int ) => U) : U ={
    func("a",1)
  }
}

