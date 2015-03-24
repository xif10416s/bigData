package org.fxi.test.ml.bean;

import java.io.Serializable;

public class UserCreditLog implements Serializable{
	private String userId;
	private String chargePhone;
	private String drawRealName;
	private String drawAccount;
	private long logTime;
	//3);//提现
	//4);//充值
	//10);//流量
	//5);//预订提现
	// 6);//预订充值
	//11);//预订流量
	private int commodityType;
	
	private int credit;
	
	public UserCreditLog() {
		
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getChargePhone() {
		return chargePhone;
	}

	public void setChargePhone(String chargePhone) {
		this.chargePhone = chargePhone;
	}

	public String getDrawRealName() {
		return drawRealName;
	}

	public void setDrawRealName(String drawRealName) {
		this.drawRealName = drawRealName;
	}

	public String getDrawAccount() {
		return drawAccount;
	}

	public void setDrawAccount(String drawAccount) {
		this.drawAccount = drawAccount;
	}

	public long getLogTime() {
		return logTime;
	}

	public void setLogTime(long logTime) {
		this.logTime = logTime;
	}

	public int getCommodityType() {
		return commodityType;
	}

	public void setCommodityType(int commodityType) {
		this.commodityType = commodityType;
	}

	public int getCredit() {
		return credit;
	}

	public void setCredit(int credit) {
		this.credit = credit;
	}
}
