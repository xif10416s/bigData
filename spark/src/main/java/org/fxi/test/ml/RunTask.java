package org.fxi.test.ml;

import java.io.Serializable;

public class RunTask implements Serializable {
	private String sql;
	private ResultHander handler;

	public RunTask(String sql, ResultHander handler) {
		this.sql = sql;
		this.handler = handler;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public ResultHander getHandler() {
		return handler;
	}

	public void setHandler(ResultHander handler) {
		this.handler = handler;
	}
}
