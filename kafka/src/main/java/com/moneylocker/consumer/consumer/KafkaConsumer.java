package com.moneylocker.consumer.consumer;

import java.io.IOException;
import java.util.HashMap;
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

public class KafkaConsumer {

	public String prop;

	private ConsumerConnector consumer;

	private String topic;

	private MessageHandler messageHandler;

	private String numThreads;

	private ExecutorService executor;

	public String getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(String numThreads) {
		this.numThreads = numThreads;
	}

	public KafkaConsumer() {

	}

	public void start() throws IOException {
		Properties props = new Properties();
		props.load(KafkaConsumer.class.getClassLoader().getResourceAsStream((prop)));

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(topicCountMap.get(topic));

		int threadNumber = 0;

		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerHandler(stream, threadNumber, messageHandler, topic));
			threadNumber++;
		}
	}

	public void close() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public String getProp() {
		return prop;
	}

	public void setProp(String prop) {
		this.prop = prop;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public MessageHandler getMessageHandler() {
		return messageHandler;
	}

	public void setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}
}

class ConsumerHandler implements Runnable {

	private KafkaStream<byte[], byte[]> mStream;

	private int mThreadNumber;

	private MessageHandler messageHandler;

	private String topic;

	public ConsumerHandler(KafkaStream<byte[], byte[]> mStream, int mThreadNumber, MessageHandler messageHandler,
			String topic) {
		this.mThreadNumber = mThreadNumber;
		this.mStream = mStream;
		this.messageHandler = messageHandler;
		this.topic = topic;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = mStream.iterator();

		while (it.hasNext()) {
			messageHandler.onMessage(it.next(), mThreadNumber, topic);
		}
	}
}
