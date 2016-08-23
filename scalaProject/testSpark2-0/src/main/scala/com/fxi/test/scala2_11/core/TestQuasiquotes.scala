package com.fxi.test.scala2_11.core

import scala.reflect.api.JavaUniverse

/**
  * Created by xifei on 16-8-18.
  */
object TestQuasiquotes {
  def main(args: Array[String]) {
    val universe = reflect.runtime.universe
    import universe._
    val tree1: JavaUniverse#Tree = q"i am { a quasiquote }"
    println(tree1)
    println(tree1 match { case q"i am { a quasiquote }" => "it worked!" })

    val a: JavaUniverse#Tree = q"1 + 1"
    println(a)
  }
}
