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
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.StatCounter;
import org.fxi.spark.analytics.learn.bean.ArtistAliasBean;
import org.fxi.spark.analytics.learn.bean.ArtistDataBean;

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
				.textFile("I:/data/profiledata_06-May-2005/user_artist_data.txt");

		JavaDoubleRDD userIds = userRawArtistDataRdd
				.mapToDouble(new DoubleFunction<String>() {

					@Override
					public double call(String t) throws Exception {
						String[] split = t.split(" ");
						return Double.parseDouble(split[0]);
					}
				});

		StatCounter stats = userIds.stats();
		System.out.println(stats);

		JavaRDD<String> artistData = ctx
				.textFile("I:/data/profiledata_06-May-2005/artist_data.txt");
		JavaRDD<ArtistDataBean> artistDataMap = artistData
				.flatMap(new FlatMapFunction<String, ArtistDataBean>() {

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
				});

		JavaRDD<String> artistAlias = ctx
				.textFile("I:/data/profiledata_06-May-2005/artist_alias.txt");
		JavaPairRDD<Integer, Integer> artistAliasFlatMapToPair = artistAlias
				.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {

					@Override
					public Iterable<Tuple2<Integer, Integer>> call(String t)
							throws Exception {
						ArrayList<Tuple2<Integer, Integer>> arrayList = new ArrayList<Tuple2<Integer, Integer>>();
						String[] split = t.split("	");
						if (split.length != 2 || StringUtils.isEmpty(split[0]))
							return arrayList;

						arrayList.add(new Tuple2(Integer.parseInt(split[0]),
								Integer.parseInt(split[1])));

						return arrayList;

					}
				});
		
		List<Integer> lookup = artistAliasFlatMapToPair.lookup(6803336);
		System.out.println(lookup.get(0));

		ctx.stop();
	}
}
