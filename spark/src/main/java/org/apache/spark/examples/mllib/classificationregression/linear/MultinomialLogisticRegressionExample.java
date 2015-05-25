package org.apache.spark.examples.mllib.classificationregression.linear;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class MultinomialLogisticRegressionExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local[6]").setAppName("SVM Classifier Example");
    SparkContext sc = new SparkContext(conf);
    String path = "data/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
//    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
//    JavaRDD<LabeledPoint> training = splits[0].cache();
//    JavaRDD<LabeledPoint> test = splits[1];
    
    JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
	training.cache();
	JavaRDD<LabeledPoint> test = data.subtract(training);

    // Run training algorithm to build the model.
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training.rdd());

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = model.predict(p.features());
          System.out.println( p.label() + "   " + prediction);
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
    double precision = metrics.precision();
    System.out.println("Precision = " + precision);
    
    BinaryClassificationMetrics metrics2 = new BinaryClassificationMetrics(
			JavaRDD.toRDD(predictionAndLabels));
	double auROC = metrics2.areaUnderROC();

	System.out.println("Area under ROC = " + auROC);

    // Save and load model
//    model.save(sc, "myModelPath");
//    LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "myModelPath");
  }
}