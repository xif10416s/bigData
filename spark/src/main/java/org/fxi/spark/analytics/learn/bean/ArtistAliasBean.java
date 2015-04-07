package org.fxi.spark.analytics.learn.bean;

import java.io.Serializable;

public class ArtistAliasBean implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1161800343086617970L;
	private int badId;
	private int goodId;

	public ArtistAliasBean() {

	}

	public ArtistAliasBean(int badId, int goodId) {
		this.badId = badId;
		this.goodId = goodId;
	}

	public int getBadId() {
		return badId;
	}

	public void setBadId(int badId) {
		this.badId = badId;
	}

	public int getGoodId() {
		return goodId;
	}

	public void setGoodId(int goodId) {
		this.goodId = goodId;
	}
}
