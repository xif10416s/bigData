package org.fxi.test.ml.bean;

import java.io.Serializable;

public class UserCredit implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String userId;
	private int activityStatus;
	private int yestodayCredit;
	private int creditRemaining;
	private int creditRevenue;
	private int actionCredit;
	private int activityCredit;
	private int recommendCredit;
	private int rightCredit;
	private int shareCredit;
	private int downloadCredit;
	private int signCredit;

	public UserCredit() {

	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public int getActivityStatus() {
		return activityStatus;
	}

	public void setActivityStatus(int activityStatus) {
		this.activityStatus = activityStatus;
	}

	public int getYestodayCredit() {
		return yestodayCredit;
	}

	public void setYestodayCredit(int yestodayCredit) {
		this.yestodayCredit = yestodayCredit;
	}

	public int getCreditRemaining() {
		return creditRemaining;
	}

	public void setCreditRemaining(int creditRemaining) {
		this.creditRemaining = creditRemaining;
	}

	public int getCreditRevenue() {
		return creditRevenue;
	}

	public void setCreditRevenue(int creditRevenue) {
		this.creditRevenue = creditRevenue;
	}

	public int getActionCredit() {
		return actionCredit;
	}

	public void setActionCredit(int actionCredit) {
		this.actionCredit = actionCredit;
	}

	public int getActivityCredit() {
		return activityCredit;
	}

	public void setActivityCredit(int activityCredit) {
		this.activityCredit = activityCredit;
	}

	public int getRecommendCredit() {
		return recommendCredit;
	}

	public void setRecommendCredit(int recommendCredit) {
		this.recommendCredit = recommendCredit;
	}

	public int getRightCredit() {
		return rightCredit;
	}

	public void setRightCredit(int rightCredit) {
		this.rightCredit = rightCredit;
	}

	public int getShareCredit() {
		return shareCredit;
	}

	public void setShareCredit(int shareCredit) {
		this.shareCredit = shareCredit;
	}

	public int getDownloadCredit() {
		return downloadCredit;
	}

	public void setDownloadCredit(int downloadCredit) {
		this.downloadCredit = downloadCredit;
	}

	public int getSignCredit() {
		return signCredit;
	}

	public void setSignCredit(int signCredit) {
		this.signCredit = signCredit;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
