package org.fxi.test.ml.bean;

import java.io.Serializable;

public class UserCreditLogDaily implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String userId;
	private String adId;
	private long credit;
	private int type ;
	private String serverLogTime;
	private String clientObtainTime;
	private String clientSyncTime;

	public long getCredit() {
		return credit;
	}

	public void setCredit(long credit) {
		this.credit = credit;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getServerLogTime() {
		return serverLogTime;
	}

	public void setServerLogTime(String serverLogTime) {
		this.serverLogTime = serverLogTime;
	}

	public String getClientObtainTime() {
		return clientObtainTime;
	}

	public void setClientObtainTime(String clientObtainTime) {
		this.clientObtainTime = clientObtainTime;
	}

	public String getClientSyncTime() {
		return clientSyncTime;
	}

	public void setClientSyncTime(String clientSyncTime) {
		this.clientSyncTime = clientSyncTime;
	}

	public UserCreditLogDaily() {

	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAdId() {
		return adId;
	}

	public void setAdId(String adId) {
		this.adId = adId;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
}
