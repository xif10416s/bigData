package org.fxi.test.mllib.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.fxi.test.ml.util.FilePathConstants;
import org.fxi.test.ml.util.StatisticsUtils;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

public class RecommendALSTest implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8453293614445285509L;

	@SuppressWarnings("serial")
	@Test
	public void loadDate() {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> appRaw = ctx.textFile(
				FilePathConstants.USER_APP_LOG_SAMPLE).cache();

		JavaRDD<Rating> trainData = appRaw.map(new Function<String, Rating>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Rating call(String v1) throws Exception {
				String[] tok = v1.split(Utils.SPLIT_TAB);
				int userID = tok[0].hashCode();
				int artistID = tok[1].hashCode();
				return new Rating(userID, artistID, 1);
			}
		}).cache();
		long count = trainData.count();
		Utils.saveToFile("I:/data/ml/result/applog/sample.txt", " count :"
				+ count + Utils.SPLIT_LINE);

		JavaRDD<Rating>[] splits = trainData.randomSplit(new double[] { 0.6,
				0.4 }, 11L);

		MatrixFactorizationModel model = buildModel(splits[0]);

		Double mse = StatisticsUtils.getMSE(splits[0], model);

		Utils.saveToFile("I:/data/ml/result/applog/sample.txt", " mse train :"
				+ mse + Utils.SPLIT_LINE);

		mse = StatisticsUtils.getMSE(splits[1], model);

		Utils.saveToFile("I:/data/ml/result/applog/sample.txt", " mse test :"
				+ mse + Utils.SPLIT_LINE);


		JavaPairRDD<Integer, String> userIDmapToPair = appRaw.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				String[] tok = t.split(Utils.SPLIT_TAB);
				int userID = tok[0].hashCode();
				return new Tuple2<Integer, String>(userID, tok[0]);
			}
		});
		
		JavaPairRDD<Integer, String> pIDmapToPair = appRaw.mapToPair(new PairFunction<String, Integer, String>() {

			@Override
			public Tuple2<Integer, String> call(String t) throws Exception {
				String[] tok = t.split(Utils.SPLIT_TAB);
				int artistID = tok[1].hashCode();
				return new Tuple2<Integer, String>(artistID, tok[1]);
			}
		});
		Rating[] recommendProducts = model.recommendProducts(
				"0000000044cafb830144cb41512f521c".hashCode(), 5);
		for (Rating r : recommendProducts) {
			Utils.saveToFile("I:/data/ml/result/applog/recommendProducts.txt",
					userIDmapToPair.lookup(r.user()).get(0) + Utils.SPLIT_TAB + pIDmapToPair.lookup(r.product()).get(0) + Utils.SPLIT_TAB
							+ r.rating() + Utils.SPLIT_LINE);
		}

		Rating[] recommendUsers = model.recommendUsers(
				"com.xunlei.kankan".hashCode(), 5);

		for (Rating r : recommendUsers) {
			Utils.saveToFile("I:/data/ml/result/applog/recommendUsers.txt",
					userIDmapToPair.lookup(r.user()).get(0) + Utils.SPLIT_TAB + pIDmapToPair.lookup(r.product()).get(0) + Utils.SPLIT_TAB
					+ r.rating() + Utils.SPLIT_LINE);
		}

		ctx.stop();
	}

	public static MatrixFactorizationModel buildModel(JavaRDD<Rating> rdd) { // 训练模型
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.trainImplicit(rdd.rdd(), rank,
				numIterations, 0.01, 1);

		return model;
	}

}
