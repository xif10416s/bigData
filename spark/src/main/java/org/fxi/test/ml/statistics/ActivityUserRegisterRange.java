package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.fxi.test.ml.bean.UserInfo;
import org.fxi.test.ml.scheams.SchemaLoader;
import org.fxi.test.ml.scheams.impl.UserCreditLogDailyDirSchemaLoader;
import org.fxi.test.ml.util.FilePathConstants;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

public class ActivityUserRegisterRange implements Serializable {

	public static final String BASE_PATH_CREDIT_LOG = "I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_%s_%s.txt";

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Test
	public void doStatistics() {

		SparkConf sparkConf = new SparkConf().setMaster("local[*]")
				.set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlCtx = new SQLContext(ctx);

		SchemaLoader userRegInfo = new SchemaLoader() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void loadSchema(JavaSparkContext ctx, SQLContext sqlCtx) {
				JavaRDD<UserInfo> userInfo = ctx.textFile(
						FilePathConstants.USER_INFO_PATH).map(
						new Function<String, UserInfo>() {
							@Override
							public UserInfo call(String line) {
								String[] parts = line.split("	");
								UserInfo userInfo = new UserInfo();
								if (parts[0].length() == 32) {
									userInfo.setId(parts[0]);
									userInfo.setRegisterTime(Long
											.parseLong(parts[17]));
								} else {
								}

								return userInfo;
							}
						});

				JavaRDD<UserInfo> filterUserInfo = userInfo
						.filter(new Function<UserInfo, Boolean>() {

							private static final long serialVersionUID = 145805894691203300L;

							@Override
							public Boolean call(UserInfo v1) throws Exception {
								return v1.getId() != null;
							}
						});
				// Apply a schema to an RDD of Java Beans and register it as a
				// table.
				DataFrame schemaPeople = sqlCtx.createDataFrame(filterUserInfo,
						UserInfo.class);
				schemaPeople.registerTempTable("userInfo");
				schemaPeople.cache();
			}

		};
		
		handler("05","13","05","12",ctx , sqlCtx);
		handler("05","12","05","11",ctx , sqlCtx);
		handler("05","11","05","10",ctx , sqlCtx);
		handler("05","10","05","09",ctx , sqlCtx);
		handler("05","09","05","08",ctx , sqlCtx);

		ctx.stop();
	}

	public void handler(String startMonth, String stratDay, String endMonth,
			String endDay, JavaSparkContext ctx, SQLContext sqlCtx) {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {
				String.format(BASE_PATH_CREDIT_LOG, startMonth, stratDay),
				String.format(BASE_PATH_CREDIT_LOG, endMonth, endDay) });
		userCreditLogDailyDirSchemaLoader.loadSchema(ctx, sqlCtx);

		DataFrame activityUserDf = sqlCtx.sql("u.registerTime from  (select distinct userId  from  userCreditLogDailyDir where clientObtainTime like '%2015-"
				+ endMonth + "-" + endDay + "%') t , userInfo u where t.userId = u.id order by u.registerTime");
		
		JavaPairRDD<String, Long> mapToPair = activityUserDf.toJavaRDD().mapToPair(new PairFunction<Row, String,Long>() {

			private static final long serialVersionUID = 7318496638126551217L;

			@Override
			public Tuple2<String, Long> call(Row t) throws Exception {
				long long1 = t.getLong(0);
				Date date = new Date(long1);
				return new Tuple2<String , Long>( new SimpleDateFormat("yyyy_MM").format(date),1L);
			}
		});
		
		JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1+v2;
			}
		});
		
		List<Tuple2<String, Long>> collect = reduceByKey.collect();
		
		for(Tuple2<String, Long> t : collect) {
			Utils.writePropertiesFile(
					"C:/ml/result/ActivityUserRegRange2015"+ endMonth + endDay +".txt",
					t._1, t._2 + "");
			
		}
		

		sqlCtx.sql("");
	}
}
