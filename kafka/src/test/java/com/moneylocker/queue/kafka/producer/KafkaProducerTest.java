package com.moneylocker.queue.kafka.producer;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class KafkaProducerTest {
	KafkaProducer kafkaProducerSync;
	KafkaProducer kafkaProducerASync;

	@Before
	public void init() {
		System.out.println("22");
		ClassPathXmlApplicationContext classPathXmlApplicationContext = new ClassPathXmlApplicationContext(
				"spring-kafka-producer.xml");
		kafkaProducerSync = (KafkaProducer) classPathXmlApplicationContext.getBean("kafkaProducerSync");

		kafkaProducerASync = (KafkaProducer) classPathXmlApplicationContext.getBean("kafkaProducerASync");
	}

	@Test
	public void test001() {
		loopSend(kafkaProducerASync,10,"test11","AS");
		loopSend(kafkaProducerASync,10,"test12","AS2");
		loopSend(kafkaProducerASync,10,"test13","AS3");
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	private  static void loopSend(KafkaProducer p , int loop , String topic , String msg) {
		UserCredit userCredit = new UserCredit();
		for (int nEvents = 0; nEvents < loop; nEvents++) {
			userCredit.setRightCredit(nEvents);
			userCredit.setUserId(msg);
			p.send(topic, nEvents + "", userCredit);
		}
	}

	@Test
	public void test002() {
		System.out.println(kafkaProducerSync);
		loopSend(kafkaProducerSync,10,"test11","S");
		loopSend(kafkaProducerSync,10,"test12","S2");
		loopSend(kafkaProducerSync,10,"test13","S3");
	}
	
	@Test
	public void test003() {
		for(int i = 0 ; i< 100 ; i++){
			kafkaProducerSync.send("test-topic", i+"", "hello world "+i);
		}
		System.out.println("---------------------------------------");
		try {
			Thread.sleep(50000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
