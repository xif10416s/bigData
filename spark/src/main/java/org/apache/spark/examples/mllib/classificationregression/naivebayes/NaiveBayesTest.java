package org.apache.spark.examples.mllib.classificationregression.naivebayes;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class NaiveBayesTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"NaiveBayesTest");
		SparkContext sc = new SparkContext(conf);
		String path = "data/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path)
				.toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.7,
				0.3 });
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), 1.0);

		JavaPairRDD<Double, Double> predictionAndLabel = testData
				.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					@Override
					public Tuple2<Double, Double> call(LabeledPoint p) {
						return new Tuple2<Double, Double>(model.predict(p
								.features()), p.label());
					}
				});
		Double testErr = 1.0
				* predictionAndLabel.filter(
						new Function<Tuple2<Double, Double>, Boolean>() {
							@Override
							public Boolean call(Tuple2<Double, Double> pl) {
								return !pl._1().equals(pl._2());
							}
						}).count() / testData.count();
		System.out.println("Test Error: " + testErr);

	}
}
