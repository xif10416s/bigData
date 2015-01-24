package com.moneylocker.queue.kafka.handler;

import kafka.message.MessageAndMetadata;


public interface IMessageHandler {
	void onMessage(MessageAndMetadata<byte[], byte[]> msgData, int m_threadNumber);
}
