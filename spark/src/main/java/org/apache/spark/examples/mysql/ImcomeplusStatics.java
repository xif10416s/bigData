package com.huaqian.statistics.spark;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.commons.math.FunctionEvaluationException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.JdbcRDD.ConnectionFactory;

import scala.Function;
import scala.Function0;

import com.huaqian.bean.UserCreditBaseInfo;

public class ImcomeplusStatics implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1352536930808675722L;

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf(true)
				.setAppName("ImcomeplusStatics");
		sparkConf.setMaster("local[6]").set("spark.executor.memory", "11g");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		ConnectionFactory connectionFactory = new JdbcRDD.ConnectionFactory() {

			private static final long serialVersionUID = 1L;

			@Override
			public Connection getConnection() throws Exception {
				Class.forName("com.mysql.jdbc.Driver").newInstance();
				return DriverManager.getConnection(
						"url", "test",
						"test");
			}

		};
		JavaRDD<Object[]> create = JdbcRDD.create(jsc, connectionFactory,
				"SELECT user_id ,credit_remaining  FROM hq_user_credit where credit_remaining >= ? and credit_remaining <= ? ", Long.MIN_VALUE,
				Long.MAX_VALUE, 1);
		
		
		
		Object[] first = create.first();
		System.out.println(create.count());
	}
}
