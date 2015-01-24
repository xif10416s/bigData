package com.moneylocker.queue.kafka.handler;

import java.util.Map.Entry;
import java.util.Set;

import kafka.message.MessageAndMetadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class LogMessageHandler implements IMessageHandler {

	@Override
	public void onMessage(MessageAndMetadata<byte[], byte[]> msgData, int m_threadNumber) {
		JSONObject parseObject = JSON.parseObject(new String(msgData.message()));
		Set<Entry<String, Object>> entrySet = parseObject.entrySet();
		for (Entry<String, Object> entry : entrySet) {
			System.out.println("Thread " + m_threadNumber + ": " + entry.getKey() + "   " + entry.getValue());
		}
	}

}
