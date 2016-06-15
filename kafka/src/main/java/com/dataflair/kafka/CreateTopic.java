package com.dataflair.kafka;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;

/**
 * This class contains logic to create topic into kafka
 *
 * @author centos
 *
 */
public class CreateTopic {

	private ZkClient zkClient = null;

	/**
	 * This is a parameterized construction
	 *
	 * @param zkConnectionString
	 *            Zookeeper connection string (EX: IP:port,IP:port)
	 */
	public CreateTopic(String zkConnectionString) {
		// Session timeout with ZK
		int sessionTimeOutMS = 10000;
		// Connection timeout with ZK
		int connectionTimeOut = 10000;
		// Create the instance of zookeeper
		zkClient = new ZkClient(zkConnectionString, sessionTimeOutMS,
				connectionTimeOut, ZKStringSerializer$.MODULE$);
	}

	/**
	 * This method contains logic to create topic into kafka
	 *
	 * @param topicName
	 *            Name of the topic
	 * @param numberOfPartition
	 *            Number of partition we want to create for given topic
	 * @param numberOfReplica
	 *            Number of replica we want to create for each partition
	 */
	public void createTopic(String topicName, int numberOfPartition,
			int numberOfReplica) {
		try {
			// Call the createTopic() method of AdminUtils class to create topic
			// into Kafka
			AdminUtils.createTopic(zkClient, topicName, numberOfPartition,
					numberOfReplica, new Properties());
			System.out.println("Topic created successfully");
		} catch (TopicExistsException topicExistsException) {
			System.out.println("Topic already exist : ");
		}

	}

	public static void main(String[] args) {
		new CreateTopic("localhost:2181").createTopic("demoTopic1", 1, 1);
	}
}
