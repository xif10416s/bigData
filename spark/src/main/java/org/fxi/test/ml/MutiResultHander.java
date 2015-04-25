package org.fxi.test.ml;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;

public interface MutiResultHander extends Serializable {
	public void  handler(DataFrame... schemas);
}
