package org.fxi.spark.analytics.learn;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.util.StatCounter;

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
		JavaRDD<String> userRawArtistDataRdd = ctx.textFile("I:/data/profiledata_06-May-2005/user_artist_data.txt");
		
		JavaDoubleRDD userIds = userRawArtistDataRdd.mapToDouble(new DoubleFunction<String>() {
			
			@Override
			public double call(String t) throws Exception {
				String[] split = t.split(" ");
				return Double.parseDouble(split[0]);
			}
		});
		
		StatCounter stats = userIds.stats();
		System.out.println(stats);

		ctx.stop();
	}
}
