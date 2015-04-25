package org.fxi.test.ml;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;

public interface ResultHander extends Serializable {
	public void handler(DataFrame schema);
}
