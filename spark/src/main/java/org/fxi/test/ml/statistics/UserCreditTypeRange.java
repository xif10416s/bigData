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
				"select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from   userCredit   limit 100  ",
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
				"select sum(creditRevenue),sum(creditRemaining) ,sum(recommendCredit) ,sum(downloadCredit) ,sum(signCredit) ,sum(actionCredit)  ,sum(rightCredit)  ,sum(shareCredit) ,sum(activityCredit) from   userCredit   limit 100  ",
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
}
