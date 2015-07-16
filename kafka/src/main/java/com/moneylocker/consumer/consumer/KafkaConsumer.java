package com.moneylocker.consumer.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.moneylocker.consumer.handler.MessageHandler;
import com.moneylocker.consumer.handler.MessageHandlerBuilder;

public class KafkaConsumer {

	private Properties properties;

	private ConsumerConnector consumer;

	private String topic;

	private MessageHandlerBuilder messageHandlerBuilder;

	private Map<String, Integer> topicMap;

	private ExecutorService executor;

	public Map<String, Integer> getTopicMap() {
		return topicMap;
	}

	public void setTopicMap(Map<String, Integer> topicMap) {
		this.topicMap = topicMap;
	}

	public KafkaConsumer() {
	}

	public void start() throws IOException {
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
		int size = 0;
		for (List<KafkaStream<byte[], byte[]>> topic : consumerMap.values()) {
			size += topic == null ? 0 : topic.size();
		}
		if (size > 0) {
			List<KafkaStream<byte[], byte[]>> partStreams = null;
			executor = Executors.newFixedThreadPool(size);
			for (String topic : consumerMap.keySet()) {
				partStreams = consumerMap.get(topic);
				if (partStreams != null) {
					for (final KafkaStream<byte[], byte[]> stream : partStreams) {
						executor.submit(new ConsumerHandler(stream, messageHandlerBuilder.build(), topic));
					}
				}
			}
		}
	}

	public void close() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties prop) {
		this.properties = prop;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public MessageHandlerBuilder getMessageHandlerBuilder() {
		return messageHandlerBuilder;
	}

	public void setMessageHandlerBuilder(MessageHandlerBuilder messageHandlerBuilder) {
		this.messageHandlerBuilder = messageHandlerBuilder;
	}

	private static class ConsumerHandler implements Runnable {

		private KafkaStream<byte[], byte[]> mStream;

		private MessageHandler messageHandler;

		private String topic;

		public ConsumerHandler(KafkaStream<byte[], byte[]> mStream, MessageHandler messageHandler, String topic) {
			this.mStream = mStream;
			this.messageHandler = messageHandler;
			this.topic = topic;
		}

		@Override
		public void run() {
			ConsumerIterator<byte[], byte[]> it = mStream.iterator();
			while (it.hasNext()) {
				messageHandler.onMessage(it.next(), topic);
			}
		}
	}
}
