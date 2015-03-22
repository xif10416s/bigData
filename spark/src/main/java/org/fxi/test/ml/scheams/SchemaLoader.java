package org.fxi.test.ml.scheams;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;

public interface SchemaLoader {
	void loadSchema(JavaSparkContext ctx, JavaSQLContext sqlCtx);
}
