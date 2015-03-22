package org.fxi.test.ml;

import java.io.Serializable;

import org.apache.spark.sql.api.java.JavaSchemaRDD;

public interface MutiResultHander extends Serializable {
	public void  handler(JavaSchemaRDD... schemas);
}
