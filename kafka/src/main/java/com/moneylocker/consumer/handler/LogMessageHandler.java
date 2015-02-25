package com.moneylocker.consumer.handler;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.moneylocker.common.cassandra.api.CassandraClient;
import com.moneylocker.consumer.constants.ParamType;
import com.moneylocker.consumer.consumer.KafkaConsumer;

public class LogMessageHandler implements MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(LogMessageHandler.class);

	private CassandraClient cassandraClient;

	private final static String SQL_SUFFIX = ".sql";

	private final static String PARAM_SUFFIX = ".param";

	private Properties props = new Properties();

	private Map<String, PreparedStatement> statmentCache = new HashMap<String, PreparedStatement>();

	private Map<String, BoundStatement> boundStatementCache = new HashMap<String, BoundStatement>();

	public LogMessageHandler() {
		try {
			props.load(KafkaConsumer.class.getClassLoader().getResourceAsStream(("logSchema.properties")));
		} catch (Exception e) {
		}
		logger.info("log achema : \n%s", props.toString());
	}

	public CassandraClient getCassandraClient() {
		return cassandraClient;
	}

	public void setCassandraClient(CassandraClient cassandraClient) {
		this.cassandraClient = cassandraClient;
	}

	@Override
	public void onMessage(MessageAndMetadata<byte[], byte[]> msgData, int m_threadNumber, String topic) {
		String sql = props.getProperty(topic + SQL_SUFFIX);
		String[] params = props.getProperty(topic + PARAM_SUFFIX).split(",");

		JSONObject parseObject = JSON.parseObject(new String(msgData.message()));
		try {
			Session session = cassandraClient.getSession();
			PreparedStatement statement = statmentCache.get(topic);
			if (statement == null) {
				statement = session.prepare(sql);
				statmentCache.put(topic, statement);
				boundStatementCache.put(topic, new BoundStatement(statement));
			}
			Object[] paramArray = new Object[params.length];
			for (int i = 0; i < params.length; i++) {
				paramArray[i] = getParam(parseObject,params[i]);
			}
			BoundStatement boundStatement = boundStatementCache.get(topic);

			boundStatement.bind(paramArray);
			session.execute(boundStatement);

		} catch (Exception e) {
			logger.error(e.toString());
		}
	}
	
	private Object getParam(JSONObject parseObject , String param) {
		String[] paramItem = param.split(":");
		ParamType valueOf = ParamType.valueOf(paramItem[1].toUpperCase().trim());
		switch (valueOf) {
		case LONG:
			return parseObject.getLong(paramItem[0].trim());
		case INT:
			return parseObject.getInteger(paramItem[0].trim());
		case STRING:
			return parseObject.getString(paramItem[0].trim());
		default:
			break;
		}
		
		return parseObject.get(paramItem[0]);
	}
	
	@SuppressWarnings("unused")
	private static class LogCSQLConfig {

		private String sql;

		private String[] params;

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public String[] getParams() {
			return params;
		}

		public void setParams(String[] params) {
			this.params = params;
		}
	}
}
