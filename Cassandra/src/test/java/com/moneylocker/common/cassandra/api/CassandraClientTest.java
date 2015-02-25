package com.moneylocker.common.cassandra.api;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.datastax.driver.core.Session;

public class CassandraClientTest {
	
	private CassandraClient cassandraClient;
	
	private ClassPathXmlApplicationContext classPathXmlApplicationContext;

	@Before
	public void init() {
		System.out.println("22");
		classPathXmlApplicationContext = new ClassPathXmlApplicationContext(
				"common-spring-context-cassandra.xml");
		cassandraClient = (CassandraClient) classPathXmlApplicationContext.getBean("cassandraClient");
	}

	@Test
	public void test0001() {
		Session connect = cassandraClient.getSession();
		com.datastax.driver.core.ResultSet execute = connect.execute("select * from mldb.user_credit;");
		System.out.println(execute.one().getString("user_id"));
	}

	@After
	public void destry() {
		classPathXmlApplicationContext.close();
		cassandraClient.close();
	}
}
