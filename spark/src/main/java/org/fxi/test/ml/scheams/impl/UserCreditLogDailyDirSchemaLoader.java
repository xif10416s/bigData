package org.fxi.test.ml.scheams.impl;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.fxi.test.ml.bean.UserCredit;
import org.fxi.test.ml.bean.UserCreditLog;
import org.fxi.test.ml.bean.UserCreditLogDaily;
import org.fxi.test.ml.scheams.SchemaLoader;
import org.fxi.test.ml.util.Utils;

import akka.util.Collections;

public class UserCreditLogDailyDirSchemaLoader implements SchemaLoader,
		Serializable {
	public String[] getPaths() {
		return paths;
	}

	public void setPaths(String[] paths) {
		this.paths = paths;
	}

	private String[] paths;
	@Override
	public void loadSchema(JavaSparkContext ctx, JavaSQLContext sqlCtx) {
		System.out
				.println("=== Data source: UserCreditLogSchemaLoader RDD ===");
		// Load a text file and convert each line to a Java Bean.
		List<JavaRDD<UserCreditLogDaily>> list = new ArrayList<JavaRDD<UserCreditLogDaily>>();
		for (int i = 0; i <paths.length; i++) {

			JavaRDD<UserCreditLogDaily> userCreditLog = ctx.textFile(
					paths[i]).map(
					new Function<String, UserCreditLogDaily>() {
						@Override
						public UserCreditLogDaily call(String line) {
							String[] parts = line.split("	");
							UserCreditLogDaily userCreditLog = new UserCreditLogDaily();
							userCreditLog.setUserId(parts[0]);
							userCreditLog.setCredit(Utils.valueOf(parts[3]));
							userCreditLog.setAdId(parts[2]);
							userCreditLog.setType(Utils.valueOf(parts[1]));
							userCreditLog.setServerLogTime(parts[4]);
							userCreditLog.setClientObtainTime(parts[5]);
							userCreditLog.setClientSyncTime(parts[6]);
							return userCreditLog;
						}
					});
			list.add(userCreditLog);
		}
		
		JavaRDD<UserCreditLogDaily> union = null;
		if(paths.length == 1) {
			union = list.get(0);
		}
		if(paths.length == 2) {
			union = ctx.union(list.get(0),list.get(1));
		}
		if(paths.length == 3) {
			union = ctx.union(list.get(0),list.get(1),list.get(2));
		}
		
		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(union,
				UserCreditLogDaily.class);
		schemaPeople.registerTempTable("userCreditLogDailyDir");
	}

	public static void main(String[] args) {
		File file = new File("I:/data/ml/20150319/hq_user_credit_log_daily");
		File[] listFiles = file.listFiles();
		for(int i = 0 ; i < listFiles.length ; i ++) {
			System.out.println(listFiles[i].getAbsolutePath());
			
		}
		
		String[] a = new String[] {};
	}
}
