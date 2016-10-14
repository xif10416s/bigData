package com.fxi.test.spark.sql.catalyst.mock

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by xifei on 16-10-14.
  */
class QueryExecutionMock(val sparkSession: SparkSessionMock, val logical: LogicalPlanMock) {
  def assertAnalyzed(): Unit = {
    try sparkSession.sessionState.analyzer.checkAnalysis(analyzed) catch {
      case e: Exception =>
        throw e
    }
  }

  lazy val analyzed: LogicalPlanMock = {
    LogUtil.doLog("＝＝＝＝遍历定义的规则解析计划＝＝＝＝＝开始"+logical.hashCode(),this.getClass)
    try{
      sparkSession.sessionState.analyzer.execute(logical)
    } finally {
      LogUtil.doLog("＝＝＝＝遍历定义的规则解析计划＝＝＝＝＝结束"+logical.hashCode(),this.getClass)
    }
  }

//  lazy val withCachedData: LogicalPlan = {
//    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
//  }

//  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

//  lazy val sparkPlan: SparkPlan = {
//    planner.plan(ReturnAnswer(optimizedPlan)).next()
//  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
//  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
//  lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

  /**
    * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
    * row format conversions as needed.
    */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
//    python.ExtractPythonUDFs,
//    PlanSubqueries(sparkSession),
//    EnsureRequirements(sparkSession.sessionState.conf),
//    CollapseCodegenStages(sparkSession.sessionState.conf),
//    ReuseExchange(sparkSession.sessionState.conf)
   )
}
