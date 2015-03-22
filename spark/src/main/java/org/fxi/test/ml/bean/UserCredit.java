package org.fxi.test.ml.bean;

import java.io.Serializable;

public class UserCredit implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String userId;
	private int activityCredit;
	private int yestodayCredit;

	public UserCredit() {

	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public int getActivityCredit() {
		return activityCredit;
	}

	public void setActivityCredit(int activityCredit) {
		this.activityCredit = activityCredit;
	}

	public int getYestodayCredit() {
		return yestodayCredit;
	}

	public void setYestodayCredit(int yestodayCredit) {
		this.yestodayCredit = yestodayCredit;
	}

}
