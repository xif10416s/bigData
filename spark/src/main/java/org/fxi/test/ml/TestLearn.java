/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fxi.test.ml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.fxi.test.ml.scheams.impl.UserCreditSchemaLoader;
import org.fxi.test.ml.scheams.impl.UserInfoSchemaLoader;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

public class TestLearn implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8168432650126102413L;

	@Before
	public void startUp() {
	}

	@Test
	public void testCount() {

		SqlHelper.executeSql("select count(*) from userInfo ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						System.out.println("count : " + first);
					}
				}, new UserInfoSchemaLoader(), new UserCreditSchemaLoader());
	}

	@Test
	public void testSelect() {
		ResultHander resultHander = new ResultHander() {
			@Override
			public void handler(JavaSchemaRDD schema) {
				List<String> nameAndCity = schema.map(
						new Function<Row, String>() {
							private static final long serialVersionUID = 1L;

							@Override
							public String call(Row row) {
								return "id: " + row.getString(0);
							}
						}).collect();
				for (String name : nameAndCity) {
					System.out.println(name);
				}

			}
		};
		SqlHelper.executeSql("select id from userInfo limit 10", resultHander,
				new UserInfoSchemaLoader(), new UserCreditSchemaLoader());
	}

	@Test
	public void testCountJoin() {
		SqlHelper
				.executeSql(
						"select count(*) from userInfo i, userCredit c where i.id = c.userId",
						new ResultHander() {

							@Override
							public void handler(JavaSchemaRDD schema) {
								Object first = schema.first();
								System.out.println("count : " + first);
							}
						}, new UserInfoSchemaLoader(),
						new UserCreditSchemaLoader());
	}

	@Test
	public void testSelectJoin() {

		ResultHander resultHander = new ResultHander() {
			@Override
			public void handler(JavaSchemaRDD schema) {
				List<String> nameAndCity = schema.map(
						new Function<Row, String>() {
							@Override
							public String call(Row row) {
								return "id: " + row.getString(0)
										+ ", activityStatus: " + row.getInt(1);
							}
						}).collect();
				for (String name : nameAndCity) {
					System.out.println(name);
				}

			}
		};
		SqlHelper
				.executeSql(
						"select i.id , c.activityStatus from userInfo i, userCredit c where i.id = c.userId order by c.activityStatus desc limit 10",
						resultHander, new UserInfoSchemaLoader(),
						new UserCreditSchemaLoader());
	}

	/**
	 * count : [2010585] >=10
	 */
	@Test
	public void testDailyActivityByActivity() {
		SqlHelper
				.executeSql(
						"select count(*) from userInfo i, userCredit c where i.id = c.userId and c.activityStatus >= 10 ",
						new ResultHander() {

							@Override
							public void handler(JavaSchemaRDD schema) {
								Object first = schema.first();
								System.out.println("count : " + first);
							}
						}, new UserInfoSchemaLoader(),
						new UserCreditSchemaLoader());
	}

	/**
	 * count : [1674001]
	 */
	@Test
	public void testDailyActivityByYestodayCredit() {
		SqlHelper
				.executeSql(
						"select count(*) from userInfo i, userCredit c where i.id = c.userId and c.yestodayCredit > 0 ",
						new ResultHander() {

							@Override
							public void handler(JavaSchemaRDD schema) {
								Object first = schema.first();
								System.out.println("count : " + first);
							}
						}, new UserInfoSchemaLoader(),
						new UserCreditSchemaLoader());
	}

	/**
	 * 
	 */
	@Test
	public void testSubSelect() {
		SqlHelper
				.executeSql(
						"select count(*) from (select i.id from userInfo i, userCredit c where i.id = c.userId and c.yestodayCredit > 0 limit 10) t",
						new ResultHander() {

							@Override
							public void handler(JavaSchemaRDD schema) {
								Object first = schema.first();
								System.out.println("count : " + first);
							}
						}, new UserInfoSchemaLoader(),
						new UserCreditSchemaLoader());
	}

	/**
be_recommended_0=10886470
be_recommended_1=[1383219]
be_recommended_2_5=[1239905]
be_recommended_6_10=[231872]
be_recommended_11_30=[93340]
be_recommended_31_50=[8295]
be_recommended_51_100=[6982]
be_recommended_101_500=[2416]
be_recommended_501_1000=[300]
be_recommended_over_5001=[6]

	 */
	@Test
	public void testUserRecommendRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=5001) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_over_5001", first + "");
						System.out.println("be recommended over 5001 : "
								+ first);
					}
				}

		));

		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=501 and count(*)<=1000) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_501_1000", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=101 and count(*)<=500) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_101_500", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=51 and count(*)<=100) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_51_100", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=31 and count(*)<=50) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_31_50", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=11 and count(*)<=30) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_11_30", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=6 and count(*)<=10) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_6_10", first + "");
						System.out.println(": " + first);
					}
				}

		));

		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) >=2 and count(*)<=5) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_2_5", first + "");
						System.out.println(": " + first);
					}
				}

		));

		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) = 1) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange.txt",
								"be_recommended_1", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}

	

	/**

mQbM3qa=126691
593864446=5821
741625876=8259
484441493=8108
	 */
	@Test
	public void testUserRecommendRangeTotalUsers() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=5001 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_over_5001", sum + "");
						System.out.println("be recommended over 5001 : "
								+ sum);
					}
				}

		));
		
		
		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=1001 and count(*)<=5000 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_1001_5000", sum + "");
					}
				}

		));

		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=501 and count(*)<=1000 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_501_1000", sum + "");
					}
				}

		));
		

		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=101 and count(*)<=500 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_101_500", sum + "");
					}
				}

		));

		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=51 and count(*)<=100 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_51_100", sum + "");
					}
				}

		));
		

		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=31 and count(*)<=50 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_31_50", sum + "");
					}
				}

		));


		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=11 and count(*)<=30 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_11_30", sum + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=6 and count(*)<=10 ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_6_10", sum + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin  in ('123456','123456789','008') group by i.userOrigin  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaRDD<Long> countList = schema.map(new Function<Row, Long>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = -4496837698919461133L;

							@Override
							public Long call(Row v1) throws Exception {
								return v1.getLong(0);
							}
						});
						Long sum =countList.reduce(new Function2<Long, Long, Long>() {
							
							@Override
							public Long call(Long v1, Long v2) throws Exception {
								// TODO Auto-generated method stub
								return v1+v2;
							}
						});
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsers.txt",
								"be_recommended_3_system", sum + "");
					}
				}

		));
		
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	
	
	@Test
	public void testUserRecommendRangeTotalUsersActivity() {
		List<RunTask> list = new ArrayList<RunTask>();
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=5001 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_over_5001", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=501 and count(*)<=5000 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_501_5000", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=101 and count(*)<=500 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_101_500", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=51 and count(*)<=100 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_51_100", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=31 and count(*)<=50 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_31_50", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=11 and count(*)<=30 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_11_30", first + "");
//					}
//				}
//
//		));
//		
//		list.add(new RunTask(
//				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=6 and count(*)<=10 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
//				new ResultHander() {
//
//					@Override
//					public void handler(JavaSchemaRDD schema) {
//						Object first = schema.first();
//						writePropertiesFile(
//								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
//								"be_recommended_6_10", first + "");
//					}
//				}
//
//		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =5 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
								"be_recommended_5", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =4 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
								"be_recommended_4", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =3 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
								"be_recommended_3", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =2  ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
								"be_recommended_2", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =1 ) t where u.recommendCode = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
								"be_recommended_1", first + "");
					}
				}

		));
		
		
		
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	// 写资源文件，含中文
	public static void writePropertiesFile(String filename, String key,
			String value) {
		File file = new File(filename);
		if(!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Properties properties = new Properties();
		try {
			InputStream fis = new FileInputStream(filename);
			properties.load(fis);
			OutputStream outputStream = new FileOutputStream(filename);
			properties.setProperty(key, value);
			properties.store(outputStream, "tt");
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * be_recommended_0=10886470
	 */
	@Test
	public void testUserRecommendRange0() {
		List<String> sqlList = new ArrayList<String>();
		sqlList.add("select u.recommendCode from userInfo u");
		sqlList.add("select  distinct i.userOrigin from userInfo i   group by i.userOrigin");
		
		SqlHelper.executeSql(sqlList, new MutiResultHander() {
			
			@Override
			public void handler(JavaSchemaRDD... schemas) {
				JavaSchemaRDD userRecommendCodeList = schemas[0];
				JavaSchemaRDD userOriginList = schemas[1];
				JavaSchemaRDD subtract = userRecommendCodeList.subtract(userOriginList);
				writePropertiesFile(
						"G:/ml/result/UserRecommendRange.txt",
						"be_recommended_0", subtract.count() + "");
			}
		}, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());
	}
	
	/**
	 * be_recommended_0=841263
	 */
	@Test
	public void testUserRecommendRange0Activity() {
		List<String> sqlList = new ArrayList<String>();
		sqlList.add("select u.recommendCode from userInfo u");
		sqlList.add("select  distinct i.userOrigin from userInfo i   group by i.userOrigin");
		sqlList.add("select i.recommendCode from userInfo i, userCredit c where i.id = c.userId and c.yestodayCredit > 0");
		SqlHelper.executeSql(sqlList, new MutiResultHander() {
			
			@Override
			public void handler(JavaSchemaRDD... schemas) {
				JavaSchemaRDD userRecommendCodeList = schemas[0];
				JavaSchemaRDD userOriginList = schemas[1];
				JavaSchemaRDD activityUserRecommendCodeList = schemas[2];
				JavaSchemaRDD subtract = userRecommendCodeList.subtract(userOriginList);
				JavaSchemaRDD intersection = subtract.intersection(activityUserRecommendCodeList);
				writePropertiesFile(
						"G:/ml/result/UserRecommendRangeTotalUsersActivity.txt",
						"be_recommended_0", intersection.count() + "");
			}
		}, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());
	}
	
	/**
	 *be_recommended_5=[115133]
be_recommended_4=[179191]
be_recommended_3=[300500]
be_recommended_2=[645081]
	 */
	@Test
	public void testUserRecommendRange2_5() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) = 2) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange2.txt",
								"be_recommended_2", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) = 3) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange2.txt",
								"be_recommended_3", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) = 4) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange2.txt",
								"be_recommended_4", first + "");
						System.out.println(": " + first);
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*)  from (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789') group by i.userOrigin having count(*) = 5) t ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendRange2.txt",
								"be_recommended_5", first + "");
						System.out.println(": " + first);
					}
				}

		));
		

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	
	/**
	 * be_recommended_0=10886470
	 */
	@Test
	public void testUserRecommendRangeSystem() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select  count(*) , i.userOrigin from userInfo i where i.userOrigin  in ('123456','123456789') group by i.userOrigin ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						List<String> rs = schema.map(
								new Function<Row, String>() {
									private static final long serialVersionUID = 1L;

									@Override
									public String call(Row row) {
										return "userOrigin: " + row.getInt(1)
												+ "  -  count : "
												+ row.getInt(0);
									}
								}).collect();
						for (String name : rs) {
							System.out.println("be_recommended_1_系统 : " + name);
						}

					}
				}

		));

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	@Test
	public void testWriteFile() {
		writePropertiesFile(
				"G:/ml/result/be_recommended_over_5001.txt",
				"be_recommended_1001_5000", 11 + "");
	}
	
	
	/**
	 * 在某个范围内被推荐人的所有推荐人的活跃人数
	 */
	@Test
	public void testUserRecommendUserRangeTotalUsersActivity() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=5001 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_over_5001", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=501 and count(*)<=5000 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_501_5000", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=101 and count(*)<=500 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_101_500", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=51 and count(*)<=100 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_51_100", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=31 and count(*)<=50 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_31_50", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=11 and count(*)<=30 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_11_30", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) >=6 and count(*)<=10 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_6_10", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =5 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_5", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =4 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_4", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =3 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_3", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =2  ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_2", first + "");
					}
				}

		));
		
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u , (select  count(*) , i.userOrigin from userInfo i where i.userOrigin not in ('123456','123456789','\\N','008') group by i.userOrigin having count(*) =1 ) t where u.userOrigin = t.userOrigin ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_1", first + "");
					}
				}

		));
		
		
		
		
		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	

	/**
	 * be_recommended_0=10886470
	 * 推荐码填了系统账号和没有填邀请码的
	 */
	@Test
	public void testUserRecommendUsersRangeSystemAndNullActivity() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u  where u.userOrigin in ('123456','123456789','008') ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_System", first + "");
					}
				}

		));
		
		list.add(new RunTask(
				"select count(*) from ( select u.id from  userInfo u  where u.userOrigin in ('\\N') ) o , userCredit c where o.id = c.userId and c.yestodayCredit > 0  ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						Object first = schema.first();
						writePropertiesFile(
								"G:/ml/result/UserRecommendUserRangeTotalUsersActivity.txt",
								"be_recommended_null", first + "");
					}
				}

		));

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());

	}
	
	/**
	 * be_recommended_0=10886470
	 * 活跃用户注册分布
	 */
	@Test
	public void testActivityUserRegisterRange() {
		List<RunTask> list = new ArrayList<RunTask>();
		list.add(new RunTask(
				"select o.registerTime from    userInfo o , userCredit c where o.id = c.userId and c.yestodayCredit > 0 order by o.registerTime asc ",
				new ResultHander() {

					@Override
					public void handler(JavaSchemaRDD schema) {
						JavaPairRDD<String, Long> flatMapToPair = schema
								.flatMapToPair(new PairFlatMapFunction<Row, String, Long>() {

									private static final long serialVersionUID = 1576005386847875524L;

									@Override
									public Iterable<Tuple2<String, Long>> call(
											Row t) throws Exception {
										List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
										Long timeLong = t.getLong(0);
										System.out.println(timeLong);
										SimpleDateFormat sdf =new  SimpleDateFormat("yyyy/MM");
										list.add(new Tuple2<String, Long>(sdf
												.format(new Date(timeLong)), 1L));
										return list;
									}
								});

						JavaPairRDD<String, Long> counts = flatMapToPair
								.reduceByKey(new Function2<Long, Long, Long>() {
									@Override
									public Long call(Long i1, Long i2) {
										return i1 + i2;
									}
								});
						List<Tuple2<String,Long>> collect = counts.collect();
						for(Tuple2<String,Long> t : collect) {
							writePropertiesFile(
									"G:/ml/result/ActivityUserRegisterRange.txt",
									t._1, t._2 + "");
						}
						
					}
				}

		));

		SqlHelper.executeSql(list, new UserInfoSchemaLoader(),
				new UserCreditSchemaLoader());
	}
	
}
