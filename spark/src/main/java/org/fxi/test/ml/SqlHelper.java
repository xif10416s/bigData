package org.fxi.test.ml;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.fxi.test.ml.scheams.SchemaLoader;

public class SqlHelper implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8978532568378983015L;

	public static void executeSql(String sql, ResultHander handler,
			SchemaLoader... loaderList) {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]").set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);

		for (SchemaLoader sl : loaderList) {
			sl.loadSchema(ctx, sqlCtx);
		}
		DataFrame scm = sqlCtx.sql(sql);

		handler.handler(scm);

		ctx.stop();
	}
	
	public static void executeSql(List<RunTask> runTasks,
			SchemaLoader... loaderList) {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);

		for (SchemaLoader sl : loaderList) {
			sl.loadSchema(ctx, sqlCtx);
		}
		
		for(RunTask run : runTasks) {
			DataFrame scm = sqlCtx.sql(run.getSql());
			run.getHandler().handler(scm);
		}

		ctx.stop();
	}
	
	
	public static void executeSql(List<String> sqlList,MutiResultHander mrsHandler,
			SchemaLoader... loaderList) {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);

		for (SchemaLoader sl : loaderList) {
			sl.loadSchema(ctx, sqlCtx);
		}
		
		DataFrame[] rsjavaScm = new  DataFrame[sqlList.size()];
		int index = 0;
		for(String sql : sqlList) {
			rsjavaScm[index++] = sqlCtx.sql(sql);
		}
		
		mrsHandler.handler(rsjavaScm);

		ctx.stop();
	}
	
}
