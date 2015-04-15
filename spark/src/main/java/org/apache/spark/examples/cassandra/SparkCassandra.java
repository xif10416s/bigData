package org.apache.spark.examples.cassandra;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.fxi.spark.analytics.learn.bean.ArtistAliasBean;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class SparkCassandra {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf(true).setAppName("SparkCassandra")
				.set("spark.cassandra.connection.host", "122.144.134.67");
		sparkConf.set("spark.cassandra.keyspace", "mltp").setMaster("local[6]")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		;
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//		CassandraConnector connector = CassandraConnector.apply(jsc.getConf());
//		Session session = connector.openSession();
//		session.execute("DROP KEYSPACE IF EXISTS java_api");
//		session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
//		session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
//		session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
//		session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
		SparkContextJavaFunctions javaFunctions = CassandraJavaUtil.javaFunctions(jsc);
		javaFunctions.cassandraTable(arg0, arg1, arg2, arg3)
		CassandraJavaRDD<CassandraRow> cassandraTable = javaFunctions.cassandraTable("mltp", "mltp_ad_action_log");
		JavaRDD<Integer> flatMap = cassandraTable.flatMap(new FlatMapFunction<CassandraRow, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -2235195728960646682L;

			@Override
			public Iterable<Integer> call(CassandraRow t) throws Exception {
				Integer day = t.getInt(1);
				List<Integer> arrayList = new ArrayList<Integer>();
				if(day == 20150415) {
					arrayList.add(t.getInt(0));
				}
				
				return arrayList;
			}
		});
		
		List<Integer> collect2 = flatMap.collect();
		for(int adId : collect2) {
			System.out.println(adId);
		}
		List<CassandraRow> collect = cassandraTable.collect();
		for(CassandraRow c : collect) {
			System.out.println(c.getString(0));
		}
	}
}
