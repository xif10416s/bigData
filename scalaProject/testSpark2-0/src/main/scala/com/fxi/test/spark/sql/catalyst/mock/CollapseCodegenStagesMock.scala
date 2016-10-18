package com.fxi.test.spark.sql.catalyst.mock

import com.fxi.test.spark.sql.catalyst.mock.exectution.WholeStageCodegenExecMock
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * Created by xifei on 16-10-17.
  */
case class CollapseCodegenStagesMock(plan: SparkPlanMock) extends Rule[SparkPlanMock]{
  override def apply(plan: SparkPlanMock): SparkPlanMock = {
    LogUtil.doLog("Rule CollapseCodegenStagesMock ==> 默认配置全局代码生成，生成WholeStageCodegenExecMock全局代码生成对象",this.getClass)
    WholeStageCodegenExecMock(plan)
  }
}
