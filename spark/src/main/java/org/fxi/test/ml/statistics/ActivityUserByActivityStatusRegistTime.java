package org.fxi.test.ml.statistics;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.fxi.test.ml.ResultHander;
import org.fxi.test.ml.RunTask;
import org.fxi.test.ml.SqlHelper;
import org.fxi.test.ml.scheams.impl.UserCreditSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.fxi.test.ml.util.Utils;
import org.junit.Test;

import scala.Tuple2;

public class ActivityUserByActivityStatusRegistTime implements Serializable {
	/**
	 */
	@Test
	public void getActivityUserByActivityStatusRegistTime() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select u.registerTime from  userInfo u  , userCredit c where u.id = c.userId and c.activityStatus >= 11  ",
				new ResultHander() {

					@Override
					public void handler(DataFrame schema) {
						JavaPairRDD<String, Long> mapToPair = schema.toJavaRDD().mapToPair(new PairFunction<Row, String,Long>() {

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
						
						List<Tuple2<String, Long>> collect = reduceByKey.collect();
						
						for(Tuple2<String, Long> t : collect) {
							Utils.writePropertiesFile(
									"C:/ml/result/ActivityUserByActivityStatusRegistTime.txt",
									t._1, t._2 + "");
							
						}
						
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
	public void getActivityRangeByActivityStatus() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*), c.activityStatus from  userCredit c group by c.activityStatus ",
				new ResultHander() {

					@Override
					public void handler(DataFrame schema) {
						JavaPairRDD<Integer, Long> mapToPair = schema.toJavaRDD().mapToPair(new PairFunction<Row, Integer,Long>() {

							private static final long serialVersionUID = 7318496638126551217L;

							@Override
							public Tuple2<Integer, Long> call(Row t) throws Exception {
								return new Tuple2<Integer , Long>( t.getInt(1),t.getLong(0));
							}
						});
						
						List<Tuple2<Integer,Long>> collect = mapToPair.collect();
						
						
						for(Tuple2<Integer, Long> t : collect) {
							Utils.writePropertiesFile(
									"C:/ml/result/getActivityRangeByActivityStatus.txt",
									t._1+"", t._2 + "");
							
						}
						
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
