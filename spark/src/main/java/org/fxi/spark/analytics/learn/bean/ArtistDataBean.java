package org.fxi.spark.analytics.learn.bean;

import java.io.Serializable;

public class ArtistDataBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7227351455801317770L;
	private int artId;
	private String name;

	public ArtistDataBean() {

	}

	public ArtistDataBean(int artId, String name) {
		this.artId = artId;
		this.name = name;
	}

	public int getArtId() {
		return artId;
	}

	public void setArtId(int artId) {
		this.artId = artId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
