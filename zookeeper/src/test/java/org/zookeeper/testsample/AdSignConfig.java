package org.zookeeper.testsample;

import org.zookeeper.config.BasePropertyConfig;


public class AdSignConfig extends BasePropertyConfig {

	private int startSignHour = 4;

	public int getStartSignHour() {
		return startSignHour;
	}

	public void setStartSignHour(int startSignHour) {
		this.startSignHour = startSignHour;
	}



}
