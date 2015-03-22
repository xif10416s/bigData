package org.fxi.test.ml.scheams.impl;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.fxi.test.ml.bean.UserCredit;
import org.fxi.test.ml.scheams.SchemaLoader;

public class UserCreditSchemaLoader implements SchemaLoader, Serializable {

	@Override
	public void loadSchema(JavaSparkContext ctx, JavaSQLContext sqlCtx) {
		System.out.println("=== Data source: UserCreditSchemaLoader RDD ===");
		// Load a text file and convert each line to a Java Bean.
		JavaRDD<UserCredit> userCredit = ctx.textFile(
				"G:/ml/20150319/hq_user_credit_2015_03_19.csv").map(
				new Function<String, UserCredit>() {
					@Override
					public UserCredit call(String line) {
						String[] parts = line.split("	");
						UserCredit userCredit = new UserCredit();
						userCredit.setUserId(parts[11]);
						userCredit.setActivityCredit(Integer.valueOf(parts[30]));
						userCredit.setYestodayCredit(Integer.valueOf(parts[29]));
						return userCredit;
					}
				});

		// Apply a schema to an RDD of Java Beans and register it as a table.
		JavaSchemaRDD schemaPeople = sqlCtx.applySchema(userCredit,
				UserCredit.class);
		schemaPeople.registerTempTable("userCredit");
	}

}
