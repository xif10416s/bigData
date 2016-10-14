package com.fxi.test.spark.sql.catalyst.mock

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

/**
  * Created by xifei on 16-10-14.
  */
case class FilterMock(condition: Column, child: LogicalPlanMock)
  extends LogicalPlanMock {

  override def output: Seq[Attribute] = child.output

  override def children: Seq[LogicalPlanMock] = Nil
}
