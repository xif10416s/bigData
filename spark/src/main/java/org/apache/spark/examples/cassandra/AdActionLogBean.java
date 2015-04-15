package org.apache.spark.examples.cassandra;

import java.io.Serializable;

public class AdActionLogBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7622425088343766436L;
	private String adId;

	public AdActionLogBean() {

	}

	public String getAdId() {
		return adId;
	}

	public void setAdId(String adId) {
		this.adId = adId;
	}

}
