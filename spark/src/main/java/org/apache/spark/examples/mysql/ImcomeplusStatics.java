package com.huaqian.statistics.spark;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.rdd.JdbcRDD.ConnectionFactory;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.huaqian.bean.UserCreditBaseInfo;
import com.huaqian.bean.UserExperiencePointBaseInfo;
import com.huaqian.util.Utils;

public class ImcomeplusStatics implements Serializable {
	public static final SimpleDateFormat sdf =  new SimpleDateFormat("YYYY_MM_dd");
	public static final String PATH = "/tmp/result/ml/incomeplus_statics_"
			+ sdf.format(new Date()) + ".txt";
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
						"test", "test",
						"test!");
			}

		};

		JavaRDD<UserCreditBaseInfo> userCreditBaseInfo = JdbcRDD
				.create(jsc,
						connectionFactory,
						"SELECT user_id ,credit_remaining  FROM hq_user_credit where credit_remaining >= ? and credit_remaining <= ? ",
						Long.MIN_VALUE, Long.MAX_VALUE, 1,
						new Function<ResultSet, UserCreditBaseInfo>() {

							/**
					 * 
					 */
							private static final long serialVersionUID = 1L;

							@Override
							public UserCreditBaseInfo call(ResultSet v1)
									throws Exception {
								UserCreditBaseInfo info = new UserCreditBaseInfo();
								info.setUser_id(v1.getString(1));
								info.setCredit_remaining(v1.getLong(2));
								return info;
							}
						}).cache();

		JavaRDD<UserExperiencePointBaseInfo> userExperiencePointBaseInfo = JdbcRDD
				.create(jsc,
						connectionFactory,
						"SELECT user_id ,remaining  FROM ml_user_experience_point where remaining >= ? and remaining <= ? ",
						Long.MIN_VALUE, Long.MAX_VALUE, 1,
						new Function<ResultSet, UserExperiencePointBaseInfo>() {

							/**
					 * 
					 */
							private static final long serialVersionUID = 1L;

							@Override
							public UserExperiencePointBaseInfo call(ResultSet v1)
									throws Exception {
								UserExperiencePointBaseInfo info = new UserExperiencePointBaseInfo();
								info.setUser_id(v1.getString(1));
								info.setRemaining(v1.getLong(2));
								return info;
							}
						}).cache();

		// UserCreditBaseInfo first = userCreditBaseInfo.first();
		Utils.writePropertiesFile(PATH, "积分表数据总量", userCreditBaseInfo.count()
				+ "");
		//
		// UserExperiencePointBaseInfo first2 =
		// userExperiencePointBaseInfo.first();
		System.out.println();
		Utils.writePropertiesFile(PATH, "用户经验表数据总量",
				userExperiencePointBaseInfo.count() + "");
		SQLContext  sqlCtx = new SQLContext (jsc);
		DataFrame createDataFrame = sqlCtx.createDataFrame(userCreditBaseInfo,
				UserCreditBaseInfo.class);
		createDataFrame.registerTempTable("hq_user_credit");
		createDataFrame = sqlCtx.createDataFrame(userExperiencePointBaseInfo,
				UserExperiencePointBaseInfo.class);
		createDataFrame.registerTempTable("ml_user_experience_point");

		sqlCtx.cacheTable("hq_user_credit");
		sqlCtx.cacheTable("ml_user_experience_point");
		getTotalUserRemainingExpOver301CreditRange(sqlCtx);
	}

	/**
	 * 当前参与收益+用户>301经验值的总剩余积分分布
	 * 
	 * @param time
	 * @return
	 */
	public static void getTotalUserRemainingExpOver301CreditRange(
			SQLContext  sqlCtx) {
		DataFrame sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining <30");
		Row first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的总剩余积分分布【0-30】",
				first.isNullAt(1) ? "" : first.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【0-30】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining >=30 and  c.credit_remaining <50");
		first = (Row) sql.first();
		Utils.writePropertiesFile(
				PATH,
				"当前参与收益+用户>301经验值的总剩余积分分布【31-50】",
				first.isNullAt(1) ? "" : first.isNullAt(1) ? "" : first
						.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【31-50】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining >=50 and  c.credit_remaining <200");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的总剩余积分分布【51-200】",
				first.isNullAt(1) ? "" : first.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【51-200】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining >=200 and  c.credit_remaining <1000");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的总剩余积分分布【201-1000】",
				first.isNullAt(1) ? "" : first.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【201-1000】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining >=1000 and  c.credit_remaining <2000");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的总剩余积分分布【1001-2000】",
				first.isNullAt(1) ? "" : first.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【1001-2000】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=301 and c.credit_remaining >=2000");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的总剩余积分分布【>2001】",
				first.isNullAt(1) ? "" : first.getLong(1) + "");
		Utils.writePropertiesFile(PATH, "当前参与收益+用户>301经验值的人数分布【>2001】",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		// 每日用户剩余经验值总量
		sql = sqlCtx
				.sql("SELECT  sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=5700");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "大于5700经验值的人的可用积分总额",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		// 当前经验值分布图
		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining <=30 ");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图0-30的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");
		Utils.writePropertiesFile(PATH, "当前经验值分布图0-30用户的积分总值",
				first.isNullAt(1) ? "" : first.getLong(1) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=31 and m.remaining <=299");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图31-299的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");
		Utils.writePropertiesFile(PATH, "当前经验值分布图31-299用户的积分总值",
				first.isNullAt(1) ? "" : first.getLong(1) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >=300 and m.remaining <=6000");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图300-6000的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");
		Utils.writePropertiesFile(PATH, "当前经验值分布图300-6000用户的积分总值",
				first.isNullAt(1) ? "" : first.getLong(1) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id) , sum(c.credit_remaining) FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining >6000 ");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图大于6000",
				first.isNullAt(0) ? "" : first.getLong(0) + "");
		Utils.writePropertiesFile(PATH, "当前经验值分布图大于6000的积分总值",
				first.isNullAt(1) ? "" : first.getLong(1) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id)  FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining <=30 and c.credit_remaining >=50");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图0-30用户的积分总值大于50的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id)  FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining <=299 and m.remaining >=31 and c.credit_remaining >=50");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图31-299用户的积分总值大于50的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

		sql = sqlCtx
				.sql("SELECT count( c.user_id)  FROM ml_user_experience_point m , hq_user_credit c where  c.user_id = m.user_id and m.remaining <=299 and m.remaining >=31 and c.credit_remaining >=200");
		first = (Row) sql.first();
		Utils.writePropertiesFile(PATH, "当前经验值分布图31-299用户的积分总值大于200的人数",
				first.isNullAt(0) ? "" : first.getLong(0) + "");

	}

}
