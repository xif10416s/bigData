package org.fxi.test.ml.scheams.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.fxi.test.ml.bean.UserCredit;
import org.fxi.test.ml.bean.UserCreditLog;
import org.fxi.test.ml.bean.UserInfo;
import org.fxi.test.ml.scheams.SchemaLoader;
import org.fxi.test.ml.util.Utils;

public class UserCreditLogSchemaLoader implements SchemaLoader, Serializable {

	@Override
	public void loadSchema(JavaSparkContext ctx, JavaSQLContext sqlCtx) {
		System.out
				.println("=== Data source: UserCreditLogSchemaLoader RDD ===");
		// Load a text file and convert each line to a Java Bean.
		JavaRDD<UserCreditLog> userCreditLog = ctx
				.textFile(
						"C:/Users/fxi/Documents/20150319/20150319/hq_user_credit_log_2015_03_19.csv")
				.map(new Function<String, UserCreditLog>() {
					@Override
					public UserCreditLog call(String line) {
						String[] parts = line.split("	");
						UserCreditLog userCreditLog = new UserCreditLog();

						if (parts.length > 19 && parts[18].length() == 32) {
							userCreditLog.setUserId(parts[18]);
							userCreditLog.setLogTime(Long.parseLong(parts[16]));
							userCreditLog.setCredit(Utils.valueOf(parts[9]));
							userCreditLog.setCommodityType(Utils
									.valueOf(parts[7]));
							userCreditLog.setChargePhone(parts[20]);
							userCreditLog.setDrawRealName(parts[21]);
							userCreditLog.setDrawAccount(parts[19]);

						}

						return userCreditLog;
					}
				});
		JavaRDD<UserCreditLog> filteruserCreditLog = userCreditLog
				.filter(new Function<UserCreditLog, Boolean>() {

					private static final long serialVersionUID = 145805894691203300L;

					@Override
					public Boolean call(UserCreditLog v1) throws Exception {
						return v1.getUserId() != null;
					}
				});
		// Apply a schema to an RDD of Java Beans and register it as a table.
		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(filteruserCreditLog,
				UserCredit.class);
		schemaPeople.registerTempTable("userCreditLog");
	}

}
