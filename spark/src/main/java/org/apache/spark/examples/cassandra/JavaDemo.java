package org.apache.spark.examples.cassandra;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.cassandra.api.java.JavaCassandraSQLContext;

import scala.Tuple2;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

/**
 * @author Administrator
 * 
 *         CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy',
 *         'replication_factor': 1 }; CREATE TABLE test.kv(key text PRIMARY KEY,
 *         value int);
 * 
 *         test.kv INSERT INTO test.kv(key, value) VALUES ('key1', 1); INSERT
 *         INTO test.kv(key, value) VALUES ('key2', 2);
 */
public class JavaDemo implements Serializable {
	private transient SparkConf conf;

	private JavaDemo(SparkConf conf) {
		this.conf = conf;
	}

	private void run() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		generateData(sc);
		compute(sc);
		showResults(sc);

		sc.stop();
	}

	private void generateData(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		Session session = connector.openSession();
		session.execute("DROP KEYSPACE IF EXISTS test");
		session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
		session.execute("CREATE TABLE test.kv(key text PRIMARY KEY, value int)");

		// session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
		// session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");

		session.execute("INSERT INTO test.kv(key, value) VALUES ('key1', 1);");
		session.execute("INSERT INTO test.kv(key, value) VALUES ('key2', 2);");
		session.execute("INSERT INTO test.kv(key, value) VALUES ('key3', 3);");

		session.close();
	}

	private void compute(JavaSparkContext sc) {
		System.out
				.println("============================================================");
		SparkContextJavaFunctions javaFunctions = CassandraJavaUtil
				.javaFunctions(sc);

		CassandraJavaRDD<CassandraRow> cassandraTable = javaFunctions
				.cassandraTable("test", "kv");
		//select from cassandra
		JavaPairRDD<String, CassandraRow> keyBy = cassandraTable
				.keyBy(new Function<CassandraRow, String>() {

					@Override
					public String call(CassandraRow v1) throws Exception {
						// TODO Auto-generated method stub
						System.out.printf(
								"cassandraTable-------------------- %s %s\n",
								v1.getString("key"), v1.getInt("value"));
						return v1.getString("key");
					}
				});
		
		// spark rdd count value
		Integer count = keyBy.map(new Function<Tuple2<String,CassandraRow>, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Tuple2<String, CassandraRow> v1)
					throws Exception {
				// TODO Auto-generated method stub
				return v1._2.getInt("value");
			}
			
		}).reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		});
		
		System.out.println("total int ---------------------------------: " + count);
		

		//return dataset
		List<Tuple2<String, CassandraRow>> collect = keyBy.collect();
		for (Tuple2<String, CassandraRow> t : collect) {
			String key = t._1;
			int value = t._2.getInt("value");
			System.out.printf(
					"List<Tuple2<TestEntry,CassandraRow>> --------%s %s \n",
					key, value);
		}

		//
		cassandraTable
				.foreachPartition(new VoidFunction<Iterator<CassandraRow>>() {

					@Override
					public void call(Iterator<CassandraRow> t) throws Exception {
						while (t.hasNext()) {
							CassandraRow next = t.next();

							System.out
									.printf("foreachPartitionAsync-------------------- %s %s\n",
											next.getString("key"),
											next.getInt("value"));
						}
					}
				});
	}

	private void showResults(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());

		Session session = connector.openSession();

		PreparedStatement prepare = session.prepare("Select * from test.kv ");
		prepare.enableTracing();
		BoundStatement boundStatement = new BoundStatement(prepare);
		ResultSet execute = session.execute(boundStatement);
		for (Row row : execute) {
			System.out.printf("%s %s\n", row.getString("key"),
					row.getInt("value"));
		}

		session.close();
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Syntax: com.datastax.spark.demo.JavaDemo <Spark Master URL> <Cassandra contact point>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf();
		conf.setAppName("Java API demo");
		conf.setMaster(args[0]);
		conf.set("spark.cassandra.connection.host", args[1]);

		JavaDemo app = new JavaDemo(conf);
		app.run();
	}
}