package org.apache.spark.examples.cassandra;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class SparkCassandra {
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf(true).setAppName("SparkCassandra").set("spark.cassandra.connection.host",
				"122.144.134.67");
		sparkConf.set("spark.cassandra.keyspace", "mltp").setMaster("local[6]").setAppName("JavaSparkSQL")
				.set("spark.executor.memory", "11g");
		;
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		showResults(jsc);
	}

	@SuppressWarnings("unused")
	private void createScheam(JavaSparkContext sc) {
		CassandraConnector connector = CassandraConnector.apply(sc.getConf());
		Session session = connector.openSession();
		session.execute("DROP KEYSPACE IF EXISTS java_api");
		session.execute("CREATE KEYSPACE java_api WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
		session.execute("CREATE TABLE java_api.products (id INT PRIMARY KEY, name TEXT, parents LIST<INT>)");
		session.execute("CREATE TABLE java_api.sales (id UUID PRIMARY KEY, product INT, price DECIMAL)");
		session.execute("CREATE TABLE java_api.summaries (product INT PRIMARY KEY, summary DECIMAL)");
	}

	@SuppressWarnings("unused")
	private void generateData(JavaSparkContext sc) {
//		List<Product> products = Arrays.asList(new Product(0, "All products", Collections.<Integer> emptyList()),
//				new Product(1, "Product A", Arrays.asList(0)), new Product(4, "Product A1", Arrays.asList(0, 1)),
//				new Product(5, "Product A2", Arrays.asList(0, 1)), new Product(2, "Product B", Arrays.asList(0)),
//				new Product(6, "Product B1", Arrays.asList(0, 2)), new Product(7, "Product B2", Arrays.asList(0, 2)),
//				new Product(3, "Product C", Arrays.asList(0)), new Product(8, "Product C1", Arrays.asList(0, 3)),
//				new Product(9, "Product C2", Arrays.asList(0, 3)));

		// JavaRDD<Product> productsRDD = sc.parallelize(products);
		// CassandraJavaUtil.javaFunctions(productsRDD,
		// Product.class).saveToCassandra("java_api", "products");
	}

	private static void showResults(JavaSparkContext jsc) {
		SparkContextJavaFunctions javaFunctions = CassandraJavaUtil.javaFunctions(jsc);
		CassandraJavaRDD<CassandraRow> cassandraTable = javaFunctions.cassandraTable("mltp", "mltp_ad_action_log_test")
				.where(" day_time = ? ", "20150414");
		JavaRDD<AdActionLogBean> map = cassandraTable.map(new Function<CassandraRow, AdActionLogBean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public AdActionLogBean call(CassandraRow v1) throws Exception {
				AdActionLogBean adActionLogBean = new AdActionLogBean();
				adActionLogBean.setAdId(v1.getString(0));
				return adActionLogBean;
			}
			
		});
		JavaSQLContext jsql = new JavaSQLContext(jsc);
		JavaSchemaRDD applySchema = jsql.applySchema(map, AdActionLogBean.class);
		applySchema.registerTempTable("AdActionLogBean");
		JavaSchemaRDD sql = jsql.sql("select adId,count(*) from AdActionLogBean group by adId ");
		List<Row> collect = sql.collect();
		for (Row r : collect) {
			System.out.println(r.getString(0) + "   " + r.getLong(1));
		}

		List<CassandraRow> collect2 = cassandraTable.collect();
		for (CassandraRow c : collect2) {
			System.out.println(c.getString(0) + " " + c.getInt(3));
		}
	}
}
