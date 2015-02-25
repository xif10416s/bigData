package com.moneylocker.common.cassandra.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraClient {
	private static final Logger logger = LoggerFactory.getLogger(CassandraClient.class);
	private Cluster cluster;
	private Session session;
	private String nodes;


	public void setNodes(String nodes) {
		this.nodes = nodes;
	}

	public Session getSession() {
		return this.session;
	}

	public void connect() {
		Builder builder = Cluster.builder();
		String[] nodeList = nodes.split(",");
		if(nodeList != null && nodeList.length >=1) {
			for(String node : nodeList) {
				builder.addContactPoint(node.trim());
			}
		} else {
			builder.addContactPoint(nodes.trim());
		}
		cluster = builder.build();
		Metadata metadata = cluster.getMetadata();
		logger.info("Connected to cluster: %s\n", metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			logger.info("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
					host.getRack());
		}
		session = cluster.connect();
	}

	public void close() {
		session.close();
		cluster.close();
		logger.info("CassandraClient closed..");
	}

}
