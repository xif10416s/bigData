package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.fxi.test.ml.ResultHander;
import org.fxi.test.ml.RunTask;
import org.fxi.test.ml.SqlHelper;
import org.fxi.test.ml.scheams.impl.UserCreditSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

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
}
