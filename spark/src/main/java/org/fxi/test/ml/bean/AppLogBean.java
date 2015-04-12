package org.fxi.test.ml.bean;

import java.io.Serializable;

public class AppLogBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6411356287388767309L;

	private String userId;
	private String appName;
	private Long time;

	public AppLogBean() {

	}

	public AppLogBean(String userId,String appName ,Long time) {
		this.userId =userId;
		this.appName = appName;
		this.time = time;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}
}
