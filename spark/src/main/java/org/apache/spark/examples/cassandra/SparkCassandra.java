package org.apache.spark.examples.cassandra;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.cassandra.api.java.JavaCassandraSQLContext;

public class SparkCassandra {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf(true).setAppName("SparkCassandra")
				.set("spark.cassandra.connection.host", "122.144.134.67");
		sparkConf.set("spark.cassandra.auth.username", "hqdb0");
		sparkConf.set("spark.cassandra.auth.password", "00huaQianV2!");
		sparkConf.set("spark.cassandra.keyspace","mltp").setMaster("local[6]")
		.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");;
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		JavaCassandraSQLContext cc = new JavaCassandraSQLContext(jsc);
		
		JavaSchemaRDD sql = cc.sql("select ad_id from mltp_ad_action_log_test where action_type =1");
		
		List<String> teenagerNames = sql.map(new Function<Row, String>() {
			@Override
			public String call(Row row) {
				return "Name: " + row.getString(0);
			}
		}).collect();
		for (String name : teenagerNames) {
			System.out.println(name);
		}
	}
}
