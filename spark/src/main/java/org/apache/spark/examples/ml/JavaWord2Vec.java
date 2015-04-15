package org.apache.spark.examples.ml;

import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

import scala.Tuple2;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class JavaWord2Vec {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[4]").setAppName(
				"JavaWord2Vec");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		String sentence = Strings.repeat("a b ", 100)
				+ Strings.repeat("a c ", 10);
		System.out.println(sentence);
		List<String> words = Lists.newArrayList(sentence.split(" "));
		List<List<String>> localDoc = Lists.newArrayList(words, words);
		JavaRDD<List<String>> doc = jsc.parallelize(localDoc);
		Word2Vec word2vec = new Word2Vec().setVectorSize(10).setSeed(42L);
		Word2VecModel model = word2vec.fit(doc);
		Tuple2<String, Object>[] syms = model.findSynonyms("a", 2);

		System.out.println(syms[0]._1());

		System.out.println(syms[0]._2());
		System.out.println(syms[1]._1());

		System.out.println(syms[1]._2());
		
		Random random = new Random();
		random.nextInt();
		 
		 System.out.println(random.nextInt(100));
		jsc.stop();
	}
}
