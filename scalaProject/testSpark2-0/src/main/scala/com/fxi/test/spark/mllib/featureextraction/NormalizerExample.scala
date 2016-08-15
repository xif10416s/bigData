/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.fxi.test.spark.mllib.featureextraction

import com.fxi.test.spark.mllib.KeyedPoint
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils
// $example off$

object NormalizerExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NormalizerExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    // $example on$
//    val data = MLUtils.loadLibSVMFile(sc, "./spark/data/mllib/sample_libsvm_data.txt")
//
//    val normalizer1 = new Normalizer()
//    val normalizer2 = new Normalizer(p = Double.PositiveInfinity)
//
//    // Each sample in data1 will be normalized using $L^2$ norm.
//    val data1 = data.map(x => (x.label, normalizer1.transform(x.features)))
//
//    // Each sample in data2 will be normalized using $L^\infty$ norm.
//    val data2 = data.map(x => (x.label, normalizer2.transform(x.features)))
//    // $example off$
//
//    println("data1: ")
//    data1.foreach(x => println(x))
//
//    println("data2: ")
//    data2.foreach(x => println(x))



    val path = "./scalaProject/scala-spark/data/test/userInfo"
    val data = sc.textFile(path).map(KeyedPoint.parse).cache()

    val normalizer1 = new Normalizer()
    val normalizer2 = new Normalizer(p = Double.PositiveInfinity)

    // Each sample in data1 will be normalized using $L^2$ norm.
    val data11 = data.map(x => (x.label, normalizer1.transform(x.features)))

    // Each sample in data2 will be normalized using $L^\infty$ norm.
    val data22 = data.map(x => (x.label, normalizer2.transform(x.features)))
    // $example off$

    println("data1: normal ")
    data11.sortByKey().collect().foreach(x => println(x))

    println("data2: normal ")
    data22.sortByKey().collect().foreach(x => println(x))

    sc.stop()
  }
}
// scalastyle:on println
