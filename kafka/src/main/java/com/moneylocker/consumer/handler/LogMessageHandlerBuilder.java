package com.moneylocker.consumer.handler;

import com.moneylocker.common.cassandra.api.CassandraClient;

public class LogMessageHandlerBuilder implements MessageHandlerBuilder {

	private CassandraClient cassandraClient;

	@Override
	public MessageHandler build() {
		return new LogMessageHandler(cassandraClient);
	}

	public CassandraClient getCassandraClient() {
		return cassandraClient;
	}

	public void setCassandraClient(CassandraClient cassandraClient) {
		this.cassandraClient = cassandraClient;
	}
}
