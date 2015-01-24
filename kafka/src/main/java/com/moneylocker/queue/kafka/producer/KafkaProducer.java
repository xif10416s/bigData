package com.moneylocker.queue.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import com.alibaba.fastjson.JSON;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	@SuppressWarnings("rawtypes")
	public Producer producer;
	
	private String prop;

	public String getProp() {
		return prop;
	}

	public void setProp(String prop) {
		this.prop = prop;
	}

	@SuppressWarnings("rawtypes")
	public void init() {
		Properties props = new Properties();
		try {
			props.load(KafkaProducer.class.getClassLoader().getResourceAsStream((prop)));
		} catch (Exception e) {

		}
		producer = new Producer(new ProducerConfig(props));
	}

	public void close() {
		producer.close();
	}

	@SuppressWarnings("unchecked")
	public <K, V> void send(String topic, K key, V value) {
		KeyedMessage<K, String> data = new KeyedMessage<K, String>(topic, key, JSON.toJSONString(value));
		producer.send(data);
	}

	@SuppressWarnings("unchecked")
	public <K, V> void send(Map<String, Map<K, V>> msgs) {
		List<KeyedMessage<K, String>> msgList = new ArrayList<KeyedMessage<K,String>>();
		for(String topic : msgs.keySet()) {
			for(K key : msgs.get(topic).keySet()) {
				msgList.add(new KeyedMessage<K, String>(topic, key,  JSON.toJSONString(msgs.get(topic).get(key))));
			}
		}
		producer.send(msgList);
	}

}