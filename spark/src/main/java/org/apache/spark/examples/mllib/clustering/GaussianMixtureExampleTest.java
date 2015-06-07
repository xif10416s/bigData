package org.apache.spark.examples.mllib.clustering;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;
public class GaussianMixtureExampleTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("GaussianMixture Example");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse data
		String path = "data/mllib/gmm_data.txt";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			public Vector call(String s) {
				String[] sarray = s.trim().split(" ");
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++)
					values[i] = Double.parseDouble(sarray[i]);
				return Vectors.dense(values);
			}
		});
		parsedData.cache();

		// Cluster the data into two classes using GaussianMixture
		GaussianMixtureModel gmm = new GaussianMixture().setK(2).run(
				parsedData.rdd());

		// Output the parameters of the mixture model
		for (int j = 0; j < gmm.k(); j++) {
			System.out.println(String.format("weight=%f\nmu=%s\nsigma=\n%s\n",
					gmm.weights()[j], gmm.gaussians()[j].mu(),
					gmm.gaussians()[j].sigma()));
		}
	}
}
