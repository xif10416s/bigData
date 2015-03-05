package org.apache.spark.examples.mllib;

import java.util.Arrays;
import java.util.List;
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
			System.out.println(line+"............");
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}
			return Vectors.dense(point);
		}
	}

	public static void main(String[] args) throws InterruptedException {
		// ���β���Ҫ����־��ʾ���ն���
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

		SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName(
				"JavaKMeans2");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc
				.textFile("examples/src/main/resources/kmeans_data.txt");

		JavaRDD<Vector> points = lines.map(new ParsePoint());

		int k = 2;
		int iterations = 1000;
		int runs = 1;

		KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs,
				KMeans.K_MEANS_PARALLEL());
		// ����ģ�͵����ĵ�
		System.out.println("Cluster centers:");
		for (Vector center : model.clusterCenters()) {
			System.out.println(" " + center);
		}

		// ʹ�����ƽ��֮������������ģ��
		double cost = model.computeCost(points.rdd());
		System.out.println("Cost: " + cost);

		// ʹ��ģ�Ͳ��Ե�������
		testPoint("0.2 0.2 0.2",model,sc);
		testPoint("9.0 9.0 9.0",model,sc);
		testPoint("4 3 8",model,sc);
		
		testPoint("3 2 8",model,sc);
		
		JavaRDD<Integer> rs = model.predict(points);
		List<Integer> collect = rs.collect();
		System.out.println(collect);
		//rs.saveAsTextFile("rs.txt");

		sc.stop();

	}
	
	//ʹ��ģ�Ͳ��Ե�������  
	private static void testPoint(String data , KMeansModel model,JavaSparkContext sc) {
		List<String> asList = Arrays.asList(data);
		JavaRDD<String> parallelize = sc.parallelize(asList);
		JavaRDD<Vector> map2 = parallelize.map(new ParsePoint());
		JavaRDD<Integer> predict = model.predict(map2);
		List<Integer> collect = predict.collect();
		System.out.println("Vectors"+ data+"is belongs to clusters:"
				+ collect);
	}
}
