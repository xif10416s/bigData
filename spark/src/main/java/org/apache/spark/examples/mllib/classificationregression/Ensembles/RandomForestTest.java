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
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class RandomForestTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RandomForestTest");
		SparkContext sc = new SparkContext(conf);
		String path = "data/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path)
				.toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Train a RandomForest model.
	//  Empty categoricalFeaturesInfo indicates all features are continuous.
	//1 classification
		classification(trainingData , testData);
		regression(trainingData , testData);
	// Save and load model
//	model.save(sc.sc(), "myModelPath");
//	RandomForestModel sameModel = RandomForestModel.load(sc.sc(), "myModelPath");
	}
	
	public static void classification(JavaRDD<LabeledPoint> trainingData ,JavaRDD<LabeledPoint> testData ) {
		Integer numClasses = 2;
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		Integer numTrees = 3; // Use more in practice.
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		String impurity = "gini";
		Integer maxDepth = 5;
		Integer maxBins = 32;
		Integer seed = 12345;

		final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
		  categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
		  seed);

		// Evaluate model on test instances and compute test error
		JavaPairRDD<Double, Double> predictionAndLabel =
		  testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
		    @Override
		    public Tuple2<Double, Double> call(LabeledPoint p) {
		      return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
		    }
		  });
		Double testErr =
		  1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
		    @Override
		    public Boolean call(Tuple2<Double, Double> pl) {
		      return !pl._1().equals(pl._2());
		    }
		  }).count() / testData.count();
		System.out.println("Test Error: " + testErr);
		System.out.println("Learned classification forest model:\n" + model.toDebugString());
	}
	
	public static void regression(JavaRDD<LabeledPoint> trainingData ,JavaRDD<LabeledPoint> testData ) {
		// Set parameters.
	//  Empty categoricalFeaturesInfo indicates all features are continuous.
	Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
	String impurity = "variance";
	Integer numTrees = 3; // Use more in practice.
	String featureSubsetStrategy = "auto"; // Let the algorithm choose.
	Integer maxDepth = 4;
	Integer maxBins = 32;
	Integer seed = 12345;

	// Train a RandomForest model.
	final RandomForestModel model = RandomForest.trainRegressor(trainingData,categoricalFeaturesInfo,
		 numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
			  seed);

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
	  }) / testData.count();
	System.out.println("Test Mean Squared Error: " + testMSE);
	System.out.println("Learned regression forest model:\n" + model.toDebugString());

	}
}
