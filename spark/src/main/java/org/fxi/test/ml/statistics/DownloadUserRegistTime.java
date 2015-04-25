package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.fxi.test.ml.ResultHander;
import org.fxi.test.ml.RunTask;
import org.fxi.test.ml.SqlHelper;
import org.fxi.test.ml.scheams.impl.UserCreditLogDailyDirSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

public class DownloadUserRegistTime implements Serializable {
	@Test
	public void rangeType_test() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/test.txt"});		
		
		SqlHelper.executeSql(templateTask( "test.txt", "2015-03-01"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150402() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_03.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-04-02RangeType.txt", "2015-04-02"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150401() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-04-01RangeType.txt", "2015-04-01"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150331() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-31RangeType.txt", "2015-03-31"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150330() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-30RangeType.txt", "2015-03-30"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150329() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-29RangeType.txt", "2015-03-29"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150328() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-28RangeType.txt", "2015-03-28"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150327() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-27RangeType.txt", "2015-03-27"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150326() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_26.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-26RangeType.txt", "2015-03-26"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150325() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_26.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt"});
		
		
		SqlHelper.executeSql(templateTask("2015-03-25RangeType.txt", "2015-03-25"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	
	public List<RunTask> templateTask(final String saveFileName,String targetDate) {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select distinct  t.userId  from userCreditLogDailyDir t where  t.serverLogTime like '%"+targetDate +"%' and t.type = 7 ",
				new ResultHander() {

					@Override
					public void handler(DataFrame schema) {
						schema.registerTempTable("downloadUsers");
						schema.cache();
						List<Row> collect =  schema.toJavaRDD().collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getString(0));
							//sb.append(Utils.SPLIT_TAB);
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/DownloadUserRange/users/"+ saveFileName,sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select o.registerTime from    userInfo o , downloadUsers c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(DataFrame schema) {
						StringBuffer sb = new StringBuffer();
						JavaPairRDD<String, Long> mapToPair =  schema.toJavaRDD().mapToPair(new PairFunction<Row, String,Long>() {

							private static final long serialVersionUID = 7318496638126551217L;

							@Override
							public Tuple2<String, Long> call(Row t) throws Exception {
								long long1 = t.getLong(0);
								Date date = new Date(long1);
								return new Tuple2<String , Long>( new SimpleDateFormat("yyyy/MM").format(date),1L);
							}
						});
						
						JavaPairRDD<String, Long> reduceByKey = mapToPair.reduceByKey(new Function2<Long, Long, Long>() {
							
							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						
						List<Tuple2<String, Long>> collect2 = reduceByKey.collect();
						sb = new StringBuffer();
						for(Tuple2<String, Long> t : collect2) {
							sb.append(t._1);
							sb.append(Utils.SPLIT_TAB);
							sb.append(t._2);
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/DownloadUserRange/RegTimeRange"+ saveFileName,sb.toString());
					}
				}

		));
		
		return list;
	}
	
	@Test
	public void testSome() {
		rangeType_20150325();
		rangeType_20150326();
		rangeType_20150327();
		rangeType_20150328();
		rangeType_20150329();
		rangeType_20150330();
		rangeType_20150331();
		rangeType_20150401();
		//rangeType_20150402();
		//rangeType_20150403();
	}
	
	/**
	 * 
	 */
	@Test
	public void downloadUserChanges() {
		SparkConf sparkConf = new SparkConf().setMaster("local[6]").set("spark.driver.maxResultSize", "2500m")
				.setAppName("JavaSparkSQL").set("spark.executor.memory", "11g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> rangeType20150402 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-04-02RangeType.txt");
		JavaRDD<String> rangeType20150401 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-04-01RangeType.txt");
		JavaRDD<String> rangeType20150331 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-31RangeType.txt");
		JavaRDD<String> rangeType20150330 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-30RangeType.txt");
		JavaRDD<String> rangeType20150329 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-29RangeType.txt");
		JavaRDD<String> rangeType20150328 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-28RangeType.txt");
		JavaRDD<String> rangeType20150327 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-27RangeType.txt");
		JavaRDD<String> rangeType20150326 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-26RangeType.txt");
		JavaRDD<String> rangeType20150325 = ctx.textFile("I:/data/ml/result/DownloadUserRange/users/2015-03-25RangeType.txt");
		
		JavaRDD<String> intersection = rangeType20150402.intersection(rangeType20150401);
		JavaRDD<String> intersection2 = rangeType20150402.intersection(rangeType20150331);
		JavaRDD<String> intersection3 = rangeType20150402.intersection(rangeType20150330);
		JavaRDD<String> intersection4 = rangeType20150402.intersection(rangeType20150329);
		JavaRDD<String> intersection5 = rangeType20150402.intersection(rangeType20150328);
		JavaRDD<String> intersection6 = rangeType20150402.intersection(rangeType20150327);
		JavaRDD<String> intersection7 = rangeType20150402.intersection(rangeType20150326);
		JavaRDD<String> intersection8 = rangeType20150402.intersection(rangeType20150325);
		
		StringBuffer sb = new StringBuffer();
		sb.append("20150402-20150401-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150331-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection2.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150330-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection3.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150329-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection4.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150328-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection5.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150327-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection6.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150326-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection7.count());
		sb.append(Utils.SPLIT_LINE);
		
		sb.append("20150402-20150325-intersection");
		sb.append(Utils.SPLIT_TAB);
		sb.append(intersection8.count());
		sb.append(Utils.SPLIT_LINE);
		
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/20150402DownloadIntersection.txt",sb.toString());

		
		JavaRDD<String> subtractNew = rangeType20150402.subtract(rangeType20150401);
		JavaRDD<String> subtractLOSS = rangeType20150401.subtract(rangeType20150402);
		sb = new StringBuffer();
		sb.append("20150402_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150402_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150401.subtract(rangeType20150331);
		subtractLOSS = rangeType20150331.subtract(rangeType20150401);
		sb = new StringBuffer();
		sb.append("20150401_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150401_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150331.subtract(rangeType20150330);
		subtractLOSS = rangeType20150330.subtract(rangeType20150331);
		sb = new StringBuffer();
		sb.append("20150331_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150331_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		
		subtractNew = rangeType20150330.subtract(rangeType20150329);
		subtractLOSS = rangeType20150329.subtract(rangeType20150330);
		sb = new StringBuffer();
		sb.append("20150330_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150330_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150329.subtract(rangeType20150328);
		subtractLOSS = rangeType20150328.subtract(rangeType20150329);
		sb = new StringBuffer();
		sb.append("20150329_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150329_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150328.subtract(rangeType20150327);
		subtractLOSS = rangeType20150327.subtract(rangeType20150328);
		sb = new StringBuffer();
		sb.append("20150328_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150328_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150327.subtract(rangeType20150326);
		subtractLOSS = rangeType20150326.subtract(rangeType20150327);
		sb = new StringBuffer();
		sb.append("20150327_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150327_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		subtractNew = rangeType20150326.subtract(rangeType20150325);
		subtractLOSS = rangeType20150325.subtract(rangeType20150326);
		sb = new StringBuffer();
		sb.append("20150326_NEW");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractNew.count());
		sb.append(Utils.SPLIT_TAB);
		sb.append("20150326_LOSS");
		sb.append(Utils.SPLIT_TAB);
		sb.append(subtractLOSS.count());
		sb.append(Utils.SPLIT_LINE);
		Utils.saveToFile("I:/data/ml/result/DownloadUserRange/UserDownloadChange.txt",sb.toString());
		
		ctx.stop();
	}
}
