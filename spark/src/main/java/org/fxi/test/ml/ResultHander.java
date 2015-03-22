package org.fxi.test.ml;

import java.io.Serializable;

import org.apache.spark.sql.api.java.JavaSchemaRDD;

public interface ResultHander extends Serializable {
	public void handler(JavaSchemaRDD schema);
}
