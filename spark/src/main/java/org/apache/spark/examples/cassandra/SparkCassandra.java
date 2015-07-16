package org.apache.spark.examples.cassandra;

import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.api.java.JavaSQLContext;
//import org.apache.spark.sql.api.java.JavaSchemaRDD;
//import org.apache.spark.sql.api.java.Row;


import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class SparkCassandra {
	public static void main(String[] args){
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Calculate Ad Actions  for " );
		sparkConf.set("spark.cassandra.connection.host",
				"host");
		String master = "local[*]";
		if (master != null) {
			sparkConf.setMaster(master);
		}
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		SparkContextJavaFunctions javaFunctions = CassandraJavaUtil
				.javaFunctions(jsc);

		String keySpace = "keySpace";

		String whereSql = String.format("action_day_key in (%s)");
		System.out.println(whereSql);
		CassandraJavaRDD<CassandraRow> cassandraTable = javaFunctions
				.cassandraTable(keySpace, "table")
				.where(whereSql,initParams() ).select("ad_id");
		JavaRDD<String> userRDD = cassandraTable
				.map(new Function<CassandraRow, String>() {

					private static final long serialVersionUID = 4071907564253901125L;

					@Override
					public String call(CassandraRow row) throws Exception {
						return row.getString("ad_id");
					}
				});
		Map<String, Long> dataMap = userRDD.countByValue();

		jsc.stop();
	}
	
	private static Object[] initParams(){
		Object[] params = new Object[256];
		return params;
	}
}
