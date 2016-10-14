package com.fxi.test.spark.sql.catalyst.mock

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by xifei on 16-10-14.
  */
case class LocalRelationMock(output: Seq[Attribute], data: Seq[InternalRow] = Nil) extends LogicalPlanMock {
  override def children: Seq[LogicalPlanMock] = Nil
}
