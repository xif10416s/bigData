package com.fxi.test.scala.base.reflection

import scala.language.experimental.macros
import scala.reflect.macros.Context

/**
  * Created by xifei on 16-8-23.
  */

object Macros {
  def currentLocation: Location = macro impl

  def impl(c: Context): c.Expr[Location] = {
    import c.universe._
    val pos = c.macroApplication.pos
    val clsLocation = c.mirror.staticModule("Location") // get symbol of "Location" object
    println(pos.source.path)
    println(pos.line)
    c.Expr(Apply(Ident(clsLocation), List(Literal(Constant(pos.source.path)), Literal(Constant(pos.line)), Literal(Constant(pos.column)))))
  }
}
