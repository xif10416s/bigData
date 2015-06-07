package org.apache.spark.examples.mllib.collaborativefiltering;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class ALSTest {
	public static final String outputDir ="/tmp/";
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"ALSTest");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse the data
		String path = "data/mllib/als/test.data";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
			}
		});

		// Build the recommendation model using ALS
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings),
				rank, numIterations, 0.01);

		// Evaluate the model on rating data
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings
				.map(new Function<Rating, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(Rating r) {
						return new Tuple2<Object, Object>(r.user(), r.product());
					}
				});
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
				.fromJavaRDD(model
						.predict(JavaRDD.toRDD(userProducts))
						.toJavaRDD()
						.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
							public Tuple2<Tuple2<Integer, Integer>, Double> call(
									Rating r) {
								return new Tuple2<Tuple2<Integer, Integer>, Double>(
										new Tuple2<Integer, Integer>(r.user(),
												r.product()), r.rating());
							}
						}));
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD
				.fromJavaRDD(
						ratings.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
							public Tuple2<Tuple2<Integer, Integer>, Double> call(
									Rating r) {
								return new Tuple2<Tuple2<Integer, Integer>, Double>(
										new Tuple2<Integer, Integer>(r.user(),
												r.product()), r.rating());
							}
						})).join(predictions).values();
		double MSE = JavaDoubleRDD.fromRDD(
				ratesAndPreds.map(
						new Function<Tuple2<Double, Double>, Object>() {
							public Object call(Tuple2<Double, Double> pair) {
								Double err = pair._1() - pair._2();
								return err * err;
							}
						}).rdd()).mean();
		System.out.println("Mean Squared Error = " + MSE);

		model.userFeatures().toJavaRDD().map(new FeaturesToString())
				.foreach(new VoidFunction<String>() {
					
					@Override
					public void call(String t) throws Exception {
						System.out.println("userFeatures ** "+ t);
					}
				});
		model.productFeatures().toJavaRDD().map(new FeaturesToString())
				.foreach(new VoidFunction<String>() {
					
					@Override
					public void call(String t) throws Exception {
						System.out.println("productFeatures ** "+ t);
					}
				});
		System.out.println("Final user/product features written to "
				+ outputDir);

		// Save and load model
	}
	static class FeaturesToString implements Function<Tuple2<Object, double[]>, String> {
		@Override
		public String call(Tuple2<Object, double[]> element) {
			return element._1() + "," + Arrays.toString(element._2());
		}
	}
}


