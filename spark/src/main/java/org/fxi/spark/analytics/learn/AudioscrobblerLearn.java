package org.fxi.spark.analytics.learn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.util.StatCounter;
import org.fxi.spark.analytics.learn.bean.ArtistAliasBean;
import org.fxi.spark.analytics.learn.bean.ArtistDataBean;
import org.fxi.test.ml.util.StatisticsUtils;
import org.fxi.test.ml.util.Utils;

import scala.Tuple2;

public class AudioscrobblerLearn implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4919181033031770993L;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> userRawArtistDataRdd = ctx
				.textFile("I:/data/profiledata_06-May-2005/data_sample.txt");

		JavaDoubleRDD userIds = userRawArtistDataRdd.mapToDouble(
				new DoubleFunction<String>() {

					@Override
					public double call(String t) throws Exception {
						String[] split = t.split(" ");
						return Double.parseDouble(split[0]);
					}
				}).cache();

		StatCounter stats = userIds.stats();
		System.out.println(stats);

		JavaRDD<String> artistData = ctx
				.textFile("I:/data/profiledata_06-May-2005/alias_data.txt");
		JavaRDD<ArtistDataBean> artistDataMap = artistData.flatMap(
				new FlatMapFunction<String, ArtistDataBean>() {

					@Override
					public Iterable<ArtistDataBean> call(String t)
							throws Exception {
						String[] split = t.split("	");
						ArrayList<ArtistDataBean> arrayList = new ArrayList<ArtistDataBean>();
						if (split.length != 2 || StringUtils.isEmpty(split[1])) {
							return arrayList;
						}
						arrayList.add(new ArtistDataBean(Integer
								.parseInt(split[0]), split[1]));
						return arrayList;
					}
				}).cache();

		JavaRDD<String> artistAlias = ctx
				.textFile("I:/data/profiledata_06-May-2005/artist_alias.txt");
		JavaPairRDD<Integer, Integer> artistAliasFlatMapToPair = artistAlias
				.flatMapToPair(
						new PairFlatMapFunction<String, Integer, Integer>() {

							@Override
							public Iterable<Tuple2<Integer, Integer>> call(
									String t) throws Exception {
								ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<Tuple2<Integer, Integer>>();
								String[] split = t.split("	");
								if (split.length != 2
										|| StringUtils.isEmpty(split[0]))
									return arrayList;

								arrayList.add(new Tuple2(Integer
										.parseInt(split[0]), Integer
										.parseInt(split[1])));

								return arrayList;

							}
						}).cache();

		// 广播变量
		final Broadcast<JavaPairRDD<Integer, Integer>> bArtistAlias = ctx
				.broadcast(artistAliasFlatMapToPair);
		List<Integer> lookup = artistAliasFlatMapToPair.lookup(6803336);
		System.out.println(lookup.get(0));

		List<Integer> lookup2 = bArtistAlias.value().lookup(6803336);
		System.out.println("============================" + lookup2);

		JavaRDD<Rating> trainData = userRawArtistDataRdd.map(
				new Function<String, Rating>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Rating call(String v1) throws Exception {
						String[] tok = v1.split(" ");
						int userID = Integer.parseInt(tok[0]);
						int artistID = Integer.parseInt(tok[1]);
						JavaPairRDD<Integer, Integer> value = bArtistAlias
								.getValue();
						List<Integer> lookup2 = value.lookup(artistID);
						int finalArtstId = 0;
						if (lookup2 != null && lookup2.size() == 1) {
							finalArtstId = lookup2.get(0);
						} else {
							finalArtstId = artistID;
						}
						double count = Double.parseDouble(tok[2]);
						return new Rating(userID, finalArtstId, count);
					}
				}).cache();

		int rank = 10;
		int iterations = 5;

		MatrixFactorizationModel model = ALS.train(trainData.rdd(), rank,
				iterations, 0.01, 1);

		Tuple2<Object, double[]> first = model.userFeatures().first();
		String str = first._1() + ",";
		for (double d : first._2) {
			str += d + ",";
		}
		System.out.println(str);

		Rating[] recommendUsers = model.recommendProducts(1000072 , 5);
		for (Rating a : recommendUsers) {
			Utils.saveToFile("I:/data/profiledata_06-May-2005/result/trainRs.txt",
					a.product() + " " + a.rating() + "\r\n");
		}

		MatrixFactorizationModel trainImplicit = ALS.trainImplicit(
				trainData.rdd(), rank, iterations, 0.01, 1);

		recommendUsers = trainImplicit.recommendProducts(1000072, 5);
		for (Rating a : recommendUsers) {
			Utils.saveToFile(
					"I:/data/profiledata_06-May-2005/result/trainImplicitRs.txt",
					a.product() + " " + a.rating() + "\r\n");
		}
		
		Double mse = StatisticsUtils.getMSE(trainData, trainImplicit);
		System.out.println("mse : " + mse);
		ctx.stop();
	}
}
