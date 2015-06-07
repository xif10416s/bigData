package org.apache.spark.examples.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SparkCoreTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	SparkConf sparkConf = new SparkConf().setMaster("local[*]")
			.set("spark.driver.maxResultSize", "2500m")
			.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	@Before
	public void setUp() {
		
	}

	@Test
	public void doSubtract() {

		List<String> strings1 = Arrays.asList("1", "2","3","4","5","6","7");
		List<String> strings2 = Arrays.asList("4","5","6","7","9");
		List<String> strings3 = Arrays.asList( "2","6","7");
		List<String> strings4 = Arrays.asList("1", "2","4","5","8","9");
		JavaRDD<String> s1 = ctx.parallelize(strings1);
		JavaRDD<String> s2 = ctx.parallelize(strings2);
		JavaRDD<String> s3 = ctx.parallelize(strings3);
		JavaRDD<String> s4 = ctx.parallelize(strings4);
		
		JavaRDD<String> subtract = s1.subtract(s2).subtract(s3).subtract(s4);
		
		List<String> collect = subtract.collect();
		for(String s : collect) {
			System.out.println(s);
		}
	}
	
	@After
	public void after() {
		ctx.stop();
	}
}
