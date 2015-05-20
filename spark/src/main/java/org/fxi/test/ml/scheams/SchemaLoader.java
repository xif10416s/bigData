package org.fxi.test.ml.scheams;

import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public interface SchemaLoader extends Serializable {
	void loadSchema(JavaSparkContext ctx, SQLContext sqlCtx);
}
