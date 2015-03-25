package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.fxi.test.ml.ResultHander;
import org.fxi.test.ml.RunTask;
import org.fxi.test.ml.SqlHelper;
import org.fxi.test.ml.scheams.impl.UserCreditSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

public class UserCreditTypeRange implements Serializable {
	@Test
	public void getTotalCreditRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from   userCredit    ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getTotalCreditRange.txt",
								"sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit)", first + "");
						try {
							Thread.sleep(60000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

		));
		

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	
	@Test
	public void getObtainTopRightCreditUserInfo() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select rightCredit  from   userCredit   limit 100  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getObtainTopRightCreditUserInfo.txt",
								"sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit)", first + "");
						try {
							Thread.sleep(60000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

		));
		

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	@Test
	public void getRemainCreditRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=0 and creditRemaining<500  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"0<=R<500", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=500 and creditRemaining<3000  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"500<=R<3000", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=3000 and creditRemaining<5000  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"3000<=R<5000", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=5000 and creditRemaining<20000  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"5000<=R<20000", first + "");
					}
				}

		));

		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=20000 and creditRemaining<200000  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"20000<=R<200000", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from   userCredit  where creditRemaining>=200000 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						
						Utils.writePropertiesFile(
								"C:/ml/result/getRemainCreditRange.txt",
								"200000<=R", first + "");
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	
	
	/**
	 * 推荐分布的 积分总体 分布
	 */
	@Test
	public void getCreditRangeByBeRecommendedRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =1 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_1", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =2 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_2", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =3 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_3", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =4 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_4", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =5 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_5", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=6 and count(*)<=10 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_6_10", first + "");
					}
				}));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=11 and count(*)<=30 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_11_30", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=31 and count(*)<=50 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_31_50", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=51 and count(*)<=100 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_51_100", first + "");
					}
				}));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=101 and count(*)<=500 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_101_500", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=501 and count(*)<=5000 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_501_5000", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=5001 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_5001_over", first + "");
					}
				}));
		
		//m没有填写
		list.add(new RunTask(
				"select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u  where u.userOrigin in ('\\N') ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getCreditRangeByBeRecommendedRange.txt",
								"be_recommended_null", first + "");
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	
	
	/**
	 * 活跃用户推荐分布的 积分总体 分布 活跃值》=11
	 */
	@Test
	public void getActivityUserCreditRangeByBeRecommendedRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =1 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_1", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =2 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_2", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =3 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_3", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =4 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_4", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =5 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_5", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=6 and count(*)<=10 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_6_10", first + "");
					}
				}));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=11 and count(*)<=30 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_11_30", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=31 and count(*)<=50 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_31_50", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=51 and count(*)<=100 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_51_100", first + "");
					}
				}));
		
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=101 and count(*)<=500 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_101_500", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=501 and count(*)<=5000 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_501_5000", first + "");
					}
				}));
		
		list.add(new RunTask(
				" select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=5001 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.activityStatus >= 11 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_5001_over", first + "");
					}
				}));
		
		//m没有填写
		list.add(new RunTask(
				"select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from ( select u.id from  userInfo u  where u.userOrigin in ('\\N') ) o , userCredit c where o.id = c.userId  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						Utils.writePropertiesFile(
								"C:/ml/result/getActivityUserCreditRangeByBeRecommendedRange.txt",
								"be_recommended_null", first + "");
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
}
