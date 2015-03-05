package org.apache.spark.test;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class UserSideCFTest {
	private static final Pattern TAB = Pattern.compile("\t");
	public static void main(String[] args) {

		UserSideCFTest cf = new UserSideCFTest();
		JavaRDD<Rating>[] splitData = splitData();
		MatrixFactorizationModel model = cf.buildModel(splitData[0]);

		
		
	}
	
	public static JavaRDD<Rating>[] splitData() { //分割数据，一部分用于训练，一部分用于测试
		SparkConf sparkConf = new SparkConf().setAppName("Statistics")
				.setMaster("local[6]");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> data = sc.textFile("data/ml-100k/u.data");
		final Pattern SPACE = Pattern.compile(" ");
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
			@Override
			public Rating call(String line) throws Exception {
				String[] tok = TAB.split(line);
				int x = Integer.parseInt(tok[0]);
				int y = Integer.parseInt(tok[1]);
				double rating = Double.parseDouble(tok[2]);
				return new Rating(x, y, rating);
			}
		});
		JavaRDD<Rating>[] splits = ratings.randomSplit(new double[]{0.6,0.4}, 11L);
		return splits;
	}
	
	public static MatrixFactorizationModel buildModel(JavaRDD<Rating> rdd) { //训练模型
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(rdd.rdd(), rank, numIterations, 0.01);
		return model;
	}
	
	public Double getMSE(JavaRDD<Rating> ratings, MatrixFactorizationModel model) { //计算MSE
		JavaPairRDD usersProducts = ratings.mapToPair(rating -> new Tuple2<>(rating.user(), rating.product()));
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = model.predict(usersProducts.rdd())
			  .toJavaRDD()
			  .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
				  @Override
				  public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
					  return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
				  }
			  });

		JavaPairRDD<Tuple2<Integer, Integer>, Double> ratesAndPreds = ratings
			  .mapToPair(new PairFunction<Rating, Tuple2<Integer, Integer>, Double>() {
				  @Override
				  public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating rating) throws Exception {
					  return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
				  }
			  });
		JavaPairRDD joins = ratesAndPreds.join(predictions);

		return joins.mapToDouble(new DoubleFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>>>() {
			@Override
			public double call(Tuple2<Tuple2<Integer, Integer>, Tuple2<Double, Double>> o) throws Exception {
				double err = o._2()._1() - o._2()._2();
				return err * err;
			}
		}).mean();
	}

}
