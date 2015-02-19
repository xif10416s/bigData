package org.apache.spark.examples.cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

public class SparkCassandra {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf(true).setAppName("SparkCassandra").set(
				"spark.cassandra.connection.host", "122.144.134.67");
		sparkConf.set("spark.cassandra.auth.username", "hqdb0") ;         
        sparkConf.set("spark.cassandra.auth.password", "00huaQianV2!");
		SparkContext sc = new SparkContext(sparkConf);
		CassandraSQLContext cc = new CassandraSQLContext(sc);
		
		SchemaRDD sql = cc.sql("SELECT * from kv ");
		long count = sql.count();
		System.out.println(count);
	}
}
