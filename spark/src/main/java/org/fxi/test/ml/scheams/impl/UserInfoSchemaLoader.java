package org.fxi.test.ml.scheams.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.fxi.test.ml.bean.UserInfo;
import org.fxi.test.ml.scheams.SchemaLoader;
import org.fxi.test.ml.util.FilePathConstants;

public class UserInfoSchemaLoader implements SchemaLoader , Serializable {

	@Override
	public void loadSchema(JavaSparkContext ctx, JavaSQLContext sqlCtx) {
		System.out.println("=== Data source: UserInfoSchemaLoader RDD ===");
		// Load a text file and convert each line to a Java Bean.
		JavaRDD<UserInfo> userInfo = ctx.textFile(
				FilePathConstants.USER_INFO_PATH).map(
				new Function<String, UserInfo>() {
					@Override
					public UserInfo call(String line) {
						String[] parts = line.split("	");
						UserInfo userInfo = new UserInfo();
						if (parts[0].length() == 32) {
							userInfo.setId(parts[0]);
							String age = parts[5].trim();
							userInfo.setAge("\\N".equals(age) ? null : Integer
									.parseInt(age));
							userInfo.setRecommendCode(parts[2]);
							userInfo.setUserOrigin(parts[11]);
							userInfo.setRegisterTime(Long.parseLong(parts[17]));
						} else {
							// System.out.println(count + ":" + line);
							// count++;
						}

						return userInfo;
					}
				});

		JavaRDD<UserInfo> filterUserInfo = userInfo
				.filter(new Function<UserInfo, Boolean>() {

					private static final long serialVersionUID = 145805894691203300L;

					@Override
					public Boolean call(UserInfo v1) throws Exception {
						return v1.getId() != null;
					}
				});
		// Apply a schema to an RDD of Java Beans and register it as a table.
		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(filterUserInfo,
				UserInfo.class);
		schemaPeople.registerTempTable("userInfo");
	}

}
