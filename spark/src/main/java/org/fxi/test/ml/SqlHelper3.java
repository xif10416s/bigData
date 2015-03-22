package org.fxi.test.ml;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.fxi.test.ml.scheams.SchemaLoader;

public class SqlHelper3 implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8103494178961187368L;
	private SparkConf sparkConf;
	private JavaSparkContext ctx;
	private JavaSQLContext sqlCtx;

	private static SqlHelper3 instance = new SqlHelper3();

	public static SqlHelper3 getInstance(SchemaLoader... loaderList) {
		for (SchemaLoader sl : loaderList) {
			sl.loadSchema(instance.getCtx(), instance.getSqlCtx());
		}
		return instance;
	}

	private SqlHelper3() {
		init();
	}

	public void init() {
		sparkConf = new SparkConf().setMaster("local[6]")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		ctx = new JavaSparkContext(sparkConf);
		sqlCtx = new JavaSQLContext(ctx);
	}

	public JavaSparkContext getCtx() {
		return ctx;
	}

	public JavaSQLContext getSqlCtx() {
		return sqlCtx;
	}

	public void close() {
		ctx.stop();
	}
	
	public JavaSchemaRDD executeSql(String sql) {
		JavaSchemaRDD ss = sqlCtx.sql("select id from userInfo limit 10");
		List<String> nameAndCity = ss.map(new Function<Row, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) {
				return "id: " + row.getString(0);
			}
		}).collect();
		for (String name : nameAndCity) {
			System.out.println(name);
		}
		return ss;
	}
}
