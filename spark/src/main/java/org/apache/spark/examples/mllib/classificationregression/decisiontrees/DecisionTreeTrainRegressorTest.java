package org.apache.spark.examples.mllib.classificationregression.decisiontrees;

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
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class DecisionTreeTrainRegressorTest {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("SVM Classifier Example");
		SparkContext sc = new SparkContext(conf);
		String path = "data/sample_libsvm_data.txt";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path)
				.toJavaRDD();
		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Set parameters.
	//  Empty categoricalFeaturesInfo indicates all features are continuous.
	Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
	String impurity = "variance";
	Integer maxDepth = 5;
	Integer maxBins = 32;

	// Train a DecisionTree model.
	final DecisionTreeModel model = DecisionTree.trainRegressor(trainingData,
	  categoricalFeaturesInfo, impurity, maxDepth, maxBins);

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
	System.out.println("Learned regression tree model:\n" + model.toDebugString());

	// Save and load model
//	model.save(sc.sc(), "myModelPath");
//	DecisionTreeModel sameModel = DecisionTreeModel.load(sc.sc(), "myModelPath");
	}
}
