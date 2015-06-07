package org.apache.spark.examples.mllib.clustering;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

/**
 * @author Administrator k:预期的分类数 maxIterations： 最大迭代次数 initalizationMode：random
 *         initialization 或者 initialization via k-means|| runs
 *         ：运行k-means算法的次数，不保证发现全局的调优方案，在给定的数据集上运行多次后，算法给出最有聚合结果。
 *         initializationSteps ： 决定k-means算法的步骤 epsilon ： 确定距离阈值内，我们认为k-均值已经融合。
 *
 *
 *
 *
 *
 *
0.0 0.0 0.0
0.1 0.1 0.1
0.2 0.2 0.2
9.0 9.0 9.0
9.1 9.1 9.1
9.2 9.2 9.2

 *
 *
 *
 */
public class KMeansExampleTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"K-means Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse data
		String path = "data/mllib/kmeans_data.txt";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			public Vector call(String s) {
				String[] sarray = s.split(" ");
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++)
					values[i] = Double.parseDouble(sarray[i]);
				return Vectors.dense(values);
			}
		});
		parsedData.cache();

		// Cluster the data into two classes using KMeans
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters,
				numIterations);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		KMeansModel model = KMeans.train(parsedData.rdd(), numClusters,
				numIterations, 2, KMeans.K_MEANS_PARALLEL());

		System.out.println("Cluster centers:");
		for (Vector center : model.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = model.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);
		
		testPoint("0.2 0.2 0.2",model,sc);
		testPoint("9.0 9.0 9.0",model,sc);
		testPoint("4 3 8",model,sc);
		
		testPoint("3 2 8",model,sc);
		
		
	}
	
	private static void testPoint(String data , KMeansModel model,JavaSparkContext sc) {
		List<String> asList = Arrays.asList(data);
		JavaRDD<String> parallelize = sc.parallelize(asList);
		JavaRDD<Vector> map2 = parallelize.map(new ParsePoint());
		JavaRDD<Integer> predict = model.predict(map2);
		List<Integer> collect = predict.collect();
		System.out.println("Vectors"+ data+"is belongs to clusters:"
				+ collect);
	}
	
	private static class ParsePoint implements Function<String, Vector> {
		private static final Pattern SPACE = Pattern.compile(" ");

		@Override
		public Vector call(String line) {
			System.out.println(line+"............");
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}
			return Vectors.dense(point);
		}
	}
}
