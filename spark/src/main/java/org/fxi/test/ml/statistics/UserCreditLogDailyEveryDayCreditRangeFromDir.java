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

public class UserCreditLogDailyEveryDayCreditRangeFromDir implements Serializable {
	@Test
	public void testCount() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_02.txt"});
		SqlHelper.executeSql("select count(*) from userCreditLogDailyDir ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						System.out.println("count : " + first);
					}
				}, userCreditLogDailyDirSchemaLoader);
	}
	
	
	@Test
	public void rangeType_20150301() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_02.txt"});
		SqlHelper.executeSql("select type , count(*) from userCreditLogDailyDir where serverLogTime like '%2015-03-01%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150301RangeType.txt", sb.toString());
					}
				}, userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150325_26() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_26.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-25%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150325RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-25%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150325RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-26%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150326RangeType.txt", sb.toString());
					}
				}

		));
		

		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-26%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150326RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	
	@Test
	public void rangeType_20150327() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_27.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-27%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150327RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-27%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150327RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150328() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_28.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-28%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150328RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-28%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150328RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	
	@Test
	public void rangeType_20150329() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_29.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-29%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150329RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-29%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150329RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150330() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_30.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-30%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150330RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-30%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150330RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}

	@Test
	public void rangeType_20150331() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_31.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-03-31%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150331RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-03-31%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150331RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}

	@Test
	public void rangeType_20150401() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-04-01%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150401RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-04-01%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150401RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}

	@Test
	public void rangeType_20150402() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_03.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-04-02%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150402RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-04-02%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150402RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType_20150403() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_02.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_04_03.txt"});
		
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type in(1,2) and clientObtainTime like '%2015-04-03%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150403RangeType.txt", sb.toString());
					}
				}

		));
		
		list.add(new RunTask(
				"select type , count(*) from userCreditLogDailyDir where type not in(1,2) and serverLogTime like '%2015-04-03%' group by type",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						StringBuffer sb = new StringBuffer();
						for(Row r : collect) {
							sb.append(r.getInt(0));
							sb.append(Utils.SPLIT_TAB);
							sb.append(r.getLong(1));
							sb.append(Utils.SPLIT_LINE);
						}
						Utils.saveToFile("I:/data/ml/result/20150403RangeType.txt", sb.toString());
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				userCreditLogDailyDirSchemaLoader);
	}
	
	@Test
	public void rangeType2() {
		UserCreditLogDailyDirSchemaLoader userCreditLogDailyDirSchemaLoader = new UserCreditLogDailyDirSchemaLoader();
		userCreditLogDailyDirSchemaLoader.setPaths(new String[] {"I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_01.txt","I:/data/ml/20150319/hq_user_credit_log_daily/hq_user_credit_log_daily_03_02.txt"});
	
		SqlHelper.executeSql("select clientObtainTime , type from userCreditLogDailyDir where clientObtainTime like '%2015-03-01%' ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<Row> collect = schema.collect();
						for(Row r : collect)
						System.out.println("count : " + r.getString(0) +"  " + r.getInt(1));
					}
				},userCreditLogDailyDirSchemaLoader);
	}

}
