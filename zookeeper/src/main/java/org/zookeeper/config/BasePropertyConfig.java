package org.zookeeper.config;

import javax.annotation.PostConstruct;

import org.apache.commons.beanutils.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;

abstract public class BasePropertyConfig implements DynamicPropertiesHelper.PropertyConvertHandler {

	@Autowired
	private DynamicPropertiesHelperFactory dynamicPropertiesHelperFactory;

	private String propertyName;

	public String getPropertyName() {
		return propertyName;
	}

	public void setPropertyName(String propertyName) {
		this.propertyName = propertyName;
	}

	@Override
	public void convert(String key, String value) {
		try {
			BeanUtils.setProperty(this, key, value);
		} catch (Exception e) {
		}
	}

	@PostConstruct
	public void register() {
		DynamicPropertiesHelper helper = dynamicPropertiesHelperFactory.getHelper(propertyName);
		if (helper != null) {
			helper.registerHandler(this);
		}
	}
}
