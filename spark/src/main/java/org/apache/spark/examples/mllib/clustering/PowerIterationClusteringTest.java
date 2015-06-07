package org.apache.spark.examples.mllib.clustering;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;

public class PowerIterationClusteringTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"K-means Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Tuple3<Long, Long, Double>> similarities = null;

		PowerIterationClustering pic = new PowerIterationClustering().setK(2)
				.setMaxIterations(10);
		PowerIterationClusteringModel model = pic.run(similarities);

		for (PowerIterationClustering.Assignment a : model.assignments()
				.toJavaRDD().collect()) {
			System.out.println(a.id() + " -> " + a.cluster());
		}
	}
}
