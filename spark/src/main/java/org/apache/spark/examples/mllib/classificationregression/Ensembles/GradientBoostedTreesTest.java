package org.apache.spark.examples.mllib.classificationregression.Ensembles;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class GradientBoostedTreesTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(
				"RandomForestTest");
		SparkContext sc = new SparkContext(conf);
		String path = "data/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path)
				.toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.7,
				0.3 });
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Train a RandomForest model.
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		// 1 classification
		classification(trainingData, testData);
		regression(trainingData, testData,data);
		// Save and load model
		// model.save(sc.sc(), "myModelPath");
		// RandomForestModel sameModel = RandomForestModel.load(sc.sc(),
		// "myModelPath");
	}

	public static void classification(JavaRDD<LabeledPoint> trainingData,
			JavaRDD<LabeledPoint> testData) {
		// Train a GradientBoostedTrees model.
		// The defaultParams for Classification use LogLoss by default.
		BoostingStrategy boostingStrategy = BoostingStrategy
				.defaultParams("Classification");
		boostingStrategy.setNumIterations(3); // Note: Use more iterations in
												// practice.
		boostingStrategy.getTreeStrategy().setNumClasses(2);
		boostingStrategy.getTreeStrategy().setMaxDepth(5);
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(
				categoricalFeaturesInfo);

		final GradientBoostedTreesModel model = GradientBoostedTrees.train(
				trainingData, boostingStrategy);

		// Evaluate model on test instances and compute test error
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
		System.out.println("Learned classification GBT model:\n"
				+ model.toDebugString());
	}

	public static void regression(JavaRDD<LabeledPoint> trainingData ,JavaRDD<LabeledPoint> testData , JavaRDD<LabeledPoint> data) {
	//  The defaultParams for Regression use SquaredError by default.
	BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
	boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
	boostingStrategy.getTreeStrategy().setMaxDepth(5);
	//  Empty categoricalFeaturesInfo indicates all features are continuous.
	Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
	boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

	final GradientBoostedTreesModel model =
	  GradientBoostedTrees.train(trainingData, boostingStrategy);

	// Evaluate model on test instances and compute test error
	JavaPairRDD<Double, Double> predictionAndLabel =
	  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
	    @Override
	    public Tuple2<Double, Double> call(LabeledPoint p) {
	      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
	    }
	  });
	Double testMSE =
	  predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {
	    @Override
	    public Double call(Tuple2<Double, Double> pl) {
	      Double diff = pl._1() - pl._2();
	      return diff * diff;
	    }
	  }).reduce(new Function2<Double, Double, Double>() {
	    @Override
	    public Double call(Double a, Double b) {
	      return a + b;
	    }
	  }) / data.count();
	System.out.println("Test Mean Squared Error: " + testMSE);
	System.out.println("Learned regression GBT model:\n" + model.toDebugString());
	}
}
