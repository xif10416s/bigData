package com.moneylocker.consumer.handler;

import kafka.message.MessageAndMetadata;

public interface MessageHandler {

	void onMessage(MessageAndMetadata<byte[], byte[]> msgData, String topic);
}
