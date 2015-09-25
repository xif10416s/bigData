package org.apache.spark.examples.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.fxi.test.ml.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;


public class SparkCoreTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	

	@Before
	public void setUp() {

	}

	@Test
	public void doSubtract() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		List<String> strings1 = Arrays
				.asList("1", "2", "3", "4", "5", "6", "7");
		List<String> strings2 = Arrays.asList("4", "5", "6", "7", "9");
		List<String> strings3 = Arrays.asList("2", "6", "7");
		List<String> strings4 = Arrays.asList("1", "2", "4", "5", "8", "9");
		JavaRDD<String> s1 = ctx.parallelize(strings1);
		JavaRDD<String> s2 = ctx.parallelize(strings2);
		JavaRDD<String> s3 = ctx.parallelize(strings3);
		JavaRDD<String> s4 = ctx.parallelize(strings4);

		JavaRDD<String> subtract = s1.subtract(s2).subtract(s3).subtract(s4);

		List<String> collect = subtract.collect();
		for (String s : collect) {
			System.out.println(s);
		}
		ctx.stop();
	}

	@Test
	public void doZipWithIndex() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		List<String> collect = ctx.textFile(
				String.format("/data/ml/data/app_pack_name/pack_name_list_0811.txt"))
				.zipWithIndex().map(new Function<Tuple2<String, Long>, String>() {

					@Override
					public String call(Tuple2<String, Long> v1)
							throws Exception {
						
						return v1._2+"	"+v1._1;
					}
				}).collect();

		StringBuffer sb = new StringBuffer();
		for(String s : collect){
			sb.append(s).append(Utils.SPLIT_LINE);
		}
		Utils.saveToFile("/data/ml/data/app_pack_name/pack_name_list_index.txt", sb.toString());
		ctx.stop();
	}
	
	
	@Test
	public void doRddDistinct() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		List<String> strings1 = Arrays
				.asList("1_1", "2_1", "2_2", "2_2", "3_1", "3_1", "4_1");
		List<String> strings2 = Arrays.asList("4_1","4_1","5_1");
		JavaRDD<String> s1 = ctx.parallelize(strings1);
		JavaRDD<String> s2 = ctx.parallelize(strings2);

		JavaRDD<String> subtract = s1.distinct().subtract(s2.distinct()).map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				return v1.split("_")[0];
			}
		});

		List<String> collect = subtract.collect();
		for (String s : collect) {
			System.out.println(s);
		}
		ctx.stop();
	}
	
	@Test
	public void doRddUnion() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		List<String> strings1 = Arrays
				.asList("1_1", "2_1", "2_2", "2_2", "3_1", "3_1", "4_1");
		List<String> strings2 = Arrays.asList("4_1","4_1","5_1");
		JavaRDD<String> s1 = ctx.parallelize(strings1);
		JavaRDD<String> s2 = ctx.parallelize(strings2);
		

		JavaRDD<String> s0 = ctx.emptyRDD();
		s0 = s0.union(s1);
		s0 = s0.union(s2);
		List<String> collect = s0.collect();
		for (String s : collect) {
			System.out.println(s);
		}
		ctx.stop();
	}
	
	@Test
	public void doGroupBy() {
		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		List<String> strings1 = Arrays
				.asList("1_1", "1_1", "1_2", "1_2", "3_1", "3_1", "4_1");
		JavaRDD<String> s1 = ctx.parallelize(strings1);
		

		ctx.stop();
	}

	@After
	public void after() {
	}
}
