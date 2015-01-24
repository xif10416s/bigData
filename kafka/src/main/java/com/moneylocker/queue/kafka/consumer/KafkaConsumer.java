package com.moneylocker.queue.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.moneylocker.queue.kafka.handler.IMessageHandler;

public class KafkaConsumer {

	public String prop;

	private ConsumerConnector consumer;

	private String topic;

	private String handlerClass;

	private String numThreads;


	private IMessageHandler messageHandler;

	private ExecutorService executor;

	public String getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(String numThreads) {
		this.numThreads = numThreads;
	}

	public KafkaConsumer() {

	}

	public void start() {
		Properties props = new Properties();
		try {
			props.load(KafkaConsumer.class.getClassLoader().getResourceAsStream((prop)));
		} catch (Exception e) {
			e.printStackTrace();
		}

		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		// now launch all the threads
		//
		executor = Executors.newFixedThreadPool(topicCountMap.get(topic));
		// now create an object to consume the messages
		//
		int threadNumber = 0;

		for (final KafkaStream stream : streams) {

			executor.submit(new ConsumerTest(stream, threadNumber, messageHandler));

			threadNumber++;

		}
		System.out.println("start end .....");

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

	public String getHandlerClass() {
		return handlerClass;
	}

	public void setHandlerClass(String handlerClass) {
		this.handlerClass = handlerClass;
		try {
			messageHandler = (IMessageHandler) (Class.forName(handlerClass).newInstance());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public IMessageHandler getMessageHandler() {
		return messageHandler;
	}

}

@SuppressWarnings("rawtypes")
class ConsumerTest implements Runnable {

	private KafkaStream mStream;

	private int mThreadNumber;

	private IMessageHandler messageHandler;

	public ConsumerTest(KafkaStream mStream, int mThreadNumber, IMessageHandler messageHandler) {

		this.mThreadNumber = mThreadNumber;

		this.mStream = mStream;

		this.messageHandler = messageHandler;

	}

	public void run() {

		ConsumerIterator<byte[], byte[]> it = mStream.iterator();

		while (it.hasNext()) {
			messageHandler.onMessage(it.next(), mThreadNumber);
		}

	}

}
