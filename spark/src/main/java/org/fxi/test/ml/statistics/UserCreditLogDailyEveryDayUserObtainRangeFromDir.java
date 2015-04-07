package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.fxi.test.ml.ResultHander;
import org.fxi.test.ml.RunTask;
import org.fxi.test.ml.SqlHelper;
import org.fxi.test.ml.scheams.impl.UserCreditLogDailyDirSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserCreditSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

public class UserCreditLogDailyEveryDayUserObtainRangeFromDir implements Serializable {
	
	
	@Test
	public void rangeType_test() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/test.txt"});		
		
		SqlHelper.executeSql(templateTask("userCreditLogDailyDir0301", "test.txt", "2015-03-01"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
		
		
	}
	
	@Test
	public void rangeType_20150301() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_02.txt"});		
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0301", "20150301RangeType.txt", "2015-03-01"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150325() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_26.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt"});
		List<RunTask> templateTask = templateTask( "userCreditLogDailyDir0325", "20150325RangeType.txt", "2015-03-25");
		SqlHelper.executeSql(templateTask, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150326() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_26.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt"});
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0326", "20150326RangeType.txt", "2015-03-26"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150327() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt"});
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0327", "20150327RangeType.txt", "2015-03-27"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150328() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt"});
	
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0328", "20150328RangeType.txt", "2015-03-28"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150329() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt"});
		
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0329", "20150329RangeType.txt", "2015-03-29"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150330() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt"});
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0330", "20150330RangeType.txt", "2015-03-30"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	

	@Test
	public void rangeType_20150331() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt"});
		
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0331", "20150331RangeType.txt", "2015-03-31"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150401() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt"});
		
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0401", "20150401RangeType.txt", "2015-04-01"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
		
	}
	
	@Test
	public void rangeType_20150402() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_03.txt"});
		
		
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0402", "20150402RangeType.txt", "2015-04-02"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150403() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_03.txt"});
		SqlHelper.executeSql(templateTask( "userCreditLogDailyDir0403", "20150403RangeType.txt", "2015-04-03"), new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);

		
	}
	
	public List<RunTask> templateTask(final String tempTableName,final String saveFileName,String targetDate) {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select   userId , type  from userCreditLogDailyDir where serverLogTime like '%"+targetDate +"%' ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							schema.registerTempTable(tempTableName);
							schema.cache();
							
						}
					}
				}

		));
		
		list.add(new RunTask(
				"select  distinct userId from "+tempTableName  +" where type = 1",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							String count = "1	"+schema.count()+Utils.SPLIT_LINE;
							Utils.saveToFile("I:/data/ml/result/dailyCreditUserCount/"+ saveFileName,count);
						}
					}
				}

		));
		list.add(new RunTask(
				"select  distinct userId from "+tempTableName  +" where type = 2",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							String count = "2	"+schema.count()+Utils.SPLIT_LINE;
							Utils.saveToFile("I:/data/ml/result/dailyCreditUserCount/"+ saveFileName,count);
						}
					}
				}

		));
		list.add(new RunTask(
				"select  distinct userId from "+tempTableName  +" where type = 5",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							String count = "5	"+schema.count()+Utils.SPLIT_LINE;
							Utils.saveToFile("I:/data/ml/result/dailyCreditUserCount/"+ saveFileName,count);
						}
					}
				}

		));
		list.add(new RunTask(
				"select  distinct userId from "+tempTableName  +" where type = 6",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							String count = "6	"+schema.count()+Utils.SPLIT_LINE;
							Utils.saveToFile("I:/data/ml/result/dailyCreditUserCount/"+ saveFileName,count);
						}
					}
				}

		));
		list.add(new RunTask(
				"select  distinct userId from "+tempTableName  +" where type = 7",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						if(schema.count() != 0) {
							String count = "7	"+schema.count()+Utils.SPLIT_LINE;
							Utils.saveToFile("I:/data/ml/result/dailyCreditUserCount/"+ saveFileName,count);
						}
					}
				}

		));
		
		return list;
	}
	
	@Test
	public void testSome() {
		rangeType_20150327();
		rangeType_20150328();
		rangeType_20150329();
		rangeType_20150330();
		rangeType_20150331();
		rangeType_20150401();
		rangeType_20150402();
		//rangeType_20150403();
	}
	
	

}
