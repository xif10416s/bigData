package org.fxi.test.ml.util;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class StatisticsUtils implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6467106099249948816L;

	@SuppressWarnings({ "serial", "unchecked" })
	public static Double getMSE(JavaRDD<Rating> ratings, MatrixFactorizationModel model) { // 计算MSE
		@SuppressWarnings("rawtypes")
		JavaPairRDD usersProducts = ratings
				.mapToPair(new PairFunction<Rating, Integer, Integer>() {
					@SuppressWarnings("unchecked")
					@Override
					public Tuple2<Integer, Integer> call(Rating rating)
							throws Exception {
						return new Tuple2(rating.user(), rating.product());
					}
				});
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model
				.predict(usersProducts.rdd())
				.toJavaRDD()
				.mapToPair(
						new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
							@Override
							public Tuple2<Tuple2<Integer, Integer>, Double> call(
									Rating rating) throws Exception {
								return new Tuple2(new Tuple2(rating.user(),
										rating.product()), rating.rating());
							}
						});

		JavaPairRDD<Tuple2<Integer, Integer>, Double> ratesAndPreds = ratings
				.mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
					@Override
					public Tuple2<Tuple2<Integer, Integer>, Double> call(
							Rating rating) throws Exception {
						return new Tuple2(new Tuple2(rating.user(), rating
								.product()), rating.rating());
					}
				});
		JavaPairRDD joins = ratesAndPreds.join(predictions);

		return joins
				.mapToDouble(
						new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
							@Override
							public double call(
									Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o)
									throws Exception {
								double err = o._2()._1() - o._2()._2();
								return err * err;
							}
						}).mean();
	}
}
