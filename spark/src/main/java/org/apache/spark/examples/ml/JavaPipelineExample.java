package org.apache.spark.examples.ml;

import java.io.Serializable;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;

public class JavaPipelineExample implements Serializable {
	public static void main(String[] args) {
		// Set up contexts.
		SparkConf conf = new SparkConf().setMaster("local[6]").setAppName(
				"JavaSimpleTextClassificationPipeline");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext jsql = new SQLContext(jsc);

		// Prepare training documents, which are labeled.
		List<LabeledDocument> localTraining = Lists.newArrayList(
				new LabeledDocument(0L, "a b c d e spark", 1.0),
				new LabeledDocument(1L, "b d", 0.0), new LabeledDocument(2L,
						"spark f g h", 1.0), new LabeledDocument(3L,
						"hadoop mapreduce", 0.0));
		DataFrame training = jsql.applySchema(
				jsc.parallelize(localTraining), LabeledDocument.class);

		// Configure an ML pipeline, which consists of three stages: tokenizer,
		// hashingTF, and lr.
		Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol(
				"words");
		HashingTF hashingTF = new HashingTF().setNumFeatures(1000)
				.setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
		LogisticRegression lr = new LogisticRegression().setMaxIter(10)
				.setRegParam(0.01);
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {
				tokenizer, hashingTF, lr });

		// Fit the pipeline to training documents.
		PipelineModel model = pipeline.fit(training);

		// Prepare test documents, which are unlabeled.
		List<Document> localTest = Lists.newArrayList(new Document(4L,
				"spark i j k"), new Document(5L, "l m n"), new Document(6L,
				"mapreduce spark"), new Document(7L, "apache hadoop"), new Document(8L, "spark k spa"));
		DataFrame test = jsql.applySchema(jsc.parallelize(localTest),
				Document.class);

		// Make predictions on test documents.
		model.transform(test).registerAsTable("prediction");
		DataFrame predictions = jsql
				.sql("SELECT id, text, score, prediction FROM prediction");
		for (Row r : predictions.collect()) {
			System.out.println("(" + r.get(0) + ", " + r.get(1)
					+ ") --> score=" + r.get(2) + ", prediction=" + r.get(3));
		}
	}

}
