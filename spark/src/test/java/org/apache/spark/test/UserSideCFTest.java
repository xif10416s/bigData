package org.apache.spark.test;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

public class UserSideCFTest {
	private static final Pattern TAB = Pattern.compile("\t");
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("Statistics")
				.setMaster("local[6]");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("data/ml-100k/u.data");
		final Pattern SPACE = Pattern.compile(" ");
		JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			@Override
			public Vector call(String v1) throws Exception {
				System.out.println(v1 + "............");
				String[] tok = SPACE.split(v1);
				double[] point = new double[tok.length];
				for (int i = 0; i < tok.length; ++i) {
					point[i] = Double.parseDouble(tok[i]);
				}
				return Vectors.dense(point);
			}
		});

	}
}
