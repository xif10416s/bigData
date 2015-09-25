package org.apache.spark.examples.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;
public class SparkSqlTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Before
	public void setUp() {

	}

	@Test
	public void dfGroupByTest() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);
		JavaRDD<String> people = ctx
				.textFile("examples/src/main/resources/people.txt");
		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName,
					DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(new Function<String, Row>() {
			public Row call(String record) throws Exception {
				String[] fields = record.split(",");
				return RowFactory.create(fields[0], fields[1].trim());
			}
		});

		// Apply the schema to the RDD.
		DataFrame peopleDataFrame = sqlCtx.createDataFrame(rowRDD, schema);
		System.out.println(peopleDataFrame.count());
		DataFrame sum = peopleDataFrame.groupBy("age").agg(count("age"));
		Row[] collect = sum.collect();
		for (Row r : collect) {
			System.out.println(r.get(0)+":"+r.get(1));
		}
		

	
		ctx.stop();
	}

	@After
	public void after() {
	}
}
