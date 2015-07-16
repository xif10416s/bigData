package com.moneylocker.consumer.handler;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.moneylocker.common.cassandra.api.CassandraClient;

/**
 * This class is not thread safe, make sure one instance one thread
 *
 */
public class LogMessageHandler implements MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(LogMessageHandler.class);

	private CassandraClient cassandraClient;

	private Map<String, BoundStatement> boundStatementCache = new HashMap<String, BoundStatement>();
	
	private static Map<String, LogCSQLConfig> sqlConfigMap = new HashMap<String, LogCSQLConfig>();
	
	public LogMessageHandler(CassandraClient cassandraClient) {
		this.cassandraClient = cassandraClient;
	}

	@Override
	public void onMessage(MessageAndMetadata<byte[], byte[]> msgData, String topic) {
		try {
			JSONObject parseObject = JSON.parseObject(new String(msgData.message()));
			Session session = cassandraClient.getSession();
			
			BoundStatement boundStatement = boundStatementCache.get(topic);
			
			LogCSQLConfig sqlConfig = sqlConfigMap.get(topic);
			
			if (boundStatement == null) {
				boundStatementCache.remove(topic);
				String sql = sqlConfig.getSql();
				PreparedStatement statement = session.prepare(sql);
				boundStatement = new BoundStatement(statement);
				boundStatementCache.put(topic, boundStatement);
			}

			Object[] paramArray = new Object[sqlConfig.getParamConfigs().length];
			for (int i = 0; i < sqlConfig.getParamConfigs().length; i++) {
				paramArray[i] = getParam(parseObject, sqlConfig.getParamConfigs()[i]);
			}

			boundStatement.bind(paramArray);
			session.execute(boundStatement);
		} catch (Exception e) {
			logger.error("Process Message for " + topic, e);
			logger.error(new String(msgData.message()));
		}
	}
	

	private Object getParam(JSONObject parseObject, ParamTypeConfig paramTypeConfig) {
		ParamType valueOf = ParamType.valueOf(paramTypeConfig.getType().toUpperCase());
		switch (valueOf) {
		case LONG:
				return parseObject.getLong(paramTypeConfig.getName());
		case INT:
				return parseObject.getInteger(paramTypeConfig.getName());
		case STRING:
		case TEXT:
				return parseObject.getString(paramTypeConfig.getName());
		case DATE:
		case TIMESTAMP:
				return parseObject.getDate(paramTypeConfig.getName());
		default:
			break;
		}
		return parseObject.get(paramTypeConfig.getName());
	}

	private static class ParamTypeConfig {

		private String name;

		private String type;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}
	}

	private static class LogCSQLConfig {

		private String sql;

		private ParamTypeConfig[] paramConfigs;

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public ParamTypeConfig[] getParamConfigs() {
			return paramConfigs;
		}

		public void setParamConfigs(ParamTypeConfig[] paramConfigs) {
			this.paramConfigs = paramConfigs;
		}
		
	}

	private static enum ParamType {
		LONG, INT, STRING, DATE, TEXT, TIMESTAMP
	}

	private final static String SQL_SUFFIX = "sql";

	private final static String PARAM_SUFFIX = "param";
	
	static {
		Properties props = new Properties();
		try {
			props.load(LogMessageHandler.class.getClassLoader().getResourceAsStream(("logSchema.properties")));
			logger.info("log schema : \n{}", props.toString());
			for (Entry<Object, Object> entry : props.entrySet()) {
				String key = (String) entry.getKey();
				String value = (String) entry.getValue();
				String[] keyParts = key.split("\\.");
				LogCSQLConfig sqlConfig = sqlConfigMap.get(keyParts[0]);
				if (sqlConfig == null) {
					sqlConfig = new LogCSQLConfig();
					sqlConfigMap.put(keyParts[0], sqlConfig);
				}
				if (SQL_SUFFIX.equals(keyParts[1])) {
					sqlConfig.setSql(value);
				} else if (PARAM_SUFFIX.equals(keyParts[1])) {
					String[] params = value.split(",");
					ParamTypeConfig[] paramTypeConfigs = new ParamTypeConfig[params.length];
					for (int i = 0; i < params.length; ++i) {
						String[] typeParts = params[i].split(":");
						ParamTypeConfig paramTypeConfig = new ParamTypeConfig();
						paramTypeConfig.setName(typeParts[0].trim());
						paramTypeConfig.setType(typeParts[1].trim());
						paramTypeConfigs[i] = paramTypeConfig;
					}
					sqlConfig.setParamConfigs(paramTypeConfigs);
				}
			}
			// Check input validation
			for (LogCSQLConfig sqlConfig : sqlConfigMap.values()) {
				if (StringUtils.isEmpty(sqlConfig.getSql()) || sqlConfig.getParamConfigs() == null) {
					throw new Exception("Log schema config wrong...");
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}

