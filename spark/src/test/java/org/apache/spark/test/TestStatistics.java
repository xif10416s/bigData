package org.apache.spark.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

public class TestStatistics {
	public static void main(String[] args) {
	    SparkConf sparkConf = new SparkConf().setAppName("Statistics").setMaster("local[2]");
	    final JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    JavaRDD<String> data = sc.textFile("data/statistics.txt");
	    final Pattern SPACE = Pattern.compile(" ");
	    JavaRDD<Vector> parsedData = data.map(new Function<String, Vector>() {
			@Override
			public Vector call(String v1) throws Exception {
				System.out.println(v1+"............");
				String[] tok = SPACE.split(v1);
				double[] point = new double[tok.length];
				for (int i = 0; i < tok.length; ++i) {
					point[i] = Double.parseDouble(tok[i]);
				}
				return Vectors.dense(point);
			}
	    });
	 
	    MultivariateStatisticalSummary summary = Statistics.colStats(parsedData.rdd());
	    System.out.println("均值:"+summary.mean());
	    System.out.println("方差:"+summary.variance());
	    System.out.println("非零统计量个数:"+summary.numNonzeros());
	    System.out.println("总数:"+summary.count());
	    System.out.println("最大值:"+summary.max());
	    System.out.println("最小值:"+summary.min());
	    
	    HashMap<String, Double> hashMap = new HashMap<String, Double>();
	    
	}
}
