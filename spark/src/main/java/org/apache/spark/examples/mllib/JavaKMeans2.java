package org.apache.spark.examples.mllib;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * @author fxi 
 * 0.0 0.0 0.0 
 * 0.1 0.1 0.1
 * 0.2 0.2 0.2 
 * 9.0 9.0 9.0 
 * 9.1 9.1 9.1 
 * 9.2 9.2 9.2
 */
public class JavaKMeans2 {
	private static class ParsePoint implements Function<String, Vector> {
		private static final Pattern SPACE = Pattern.compile(" ");

		@Override
		public Vector call(String line) {
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}
			return Vectors.dense(point);
		}
	}

	public static void main(String[] args) throws InterruptedException {
		// 屏蔽不必要的日志显示在终端上
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName(
				"JavaKMeans2");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc
				.textFile("C:/Users/fxi/git/spark/data/mllib/kmeans_data.txt");

		JavaRDD<Vector> points = lines.map(new ParsePoint());

		int k = 2;
		int iterations = 30;
		int runs = 1;

		KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs,
				KMeans.K_MEANS_PARALLEL());
		// 数据模型的中心点
		System.out.println("Cluster centers:");
		for (Vector center : model.clusterCenters()) {
			System.out.println(" " + center);
		}

		// 使用误差平方之和来评估数据模型
		double cost = model.computeCost(points.rdd());
		System.out.println("Cost: " + cost);

		// 使用模型测试单点数据
		testPoint("0.2 0.2 0.2",model,sc);
		testPoint("0.8 0.8 0.8",model,sc);
		testPoint("9.0 9.0 9.0",model,sc);
		
		testPoint("8 8 8",model,sc);
		
		JavaRDD<Integer> rs = model.predict(points);
		System.out.println(rs);
		//rs.saveAsTextFile("rs.txt");

		sc.stop();

	}
	
	//使用模型测试单点数据  
	private static void testPoint(String data , KMeansModel model,JavaSparkContext sc) {
		JavaRDD<String> parallelize = sc.parallelize(Arrays
				.asList(data.split(" ")));

		System.out.println("Vectors"+ data+"is belongs to clusters:"
				+ model.predict(parallelize.map(new ParsePoint())).map(
						new Function<Integer, Integer>() {

							@Override
							public Integer call(Integer v1) throws Exception {
								System.out.println(v1);
								return v1;
							}
						}));
	}
}
