package com.fxi.test.scala2_11.core

import scala.reflect.runtime.universe._
import scala.tools.scalap.scalax.rules.scalasig.{NoSymbol, ClassSymbol}

/**
  * Created by xifei on 16-8-18.
  */
object TestSymbols {
  def main(args: Array[String]): Unit = {
    val universe = reflect.runtime.universe
    universe.new
  }
}
