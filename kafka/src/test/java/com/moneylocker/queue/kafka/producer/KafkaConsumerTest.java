package com.moneylocker.queue.kafka.producer;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.moneylocker.queue.kafka.consumer.KafkaConsumer;

public class KafkaConsumerTest {
	KafkaConsumer kafkaConsumer ;

	@Before
	public void init() {
		System.out.println("22");
		ClassPathXmlApplicationContext classPathXmlApplicationContext = new ClassPathXmlApplicationContext(
				"spring-kafka-consumer.xml");
		kafkaConsumer = (KafkaConsumer) classPathXmlApplicationContext.getBean("kafkaConsumerGroup1");
	}

	@Test
	public void test001() {
//		kafkaConsumer = new KafkaConsumer();
//		kafkaConsumer.setProp("kafka-consumer.properties");
		System.out.println(kafkaConsumer);
//		kafkaConsumer.start();
		
	}
	
	public static void main(String[] args) {
		ClassPathXmlApplicationContext classPathXmlApplicationContext = new ClassPathXmlApplicationContext(
				"spring-kafka-consumer.xml");
		//KafkaConsumer kafkaConsumer = (KafkaConsumer) classPathXmlApplicationContext.getBean("kafkaConsumerGroup1Test");
	}

}
