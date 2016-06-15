package com.dataflair.kafka;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Push the data into kafka. Sync producer with default partition logic
 * @author centos
 *
 */

public class KafkaProduce {

	public void produce(long numberOfEvents, String brokers, String topicName, String zkConnectionString) {
		try {
			Properties props = new Properties();
			// 192.168.65.128:9092,192.168.65.129:9092
			props.put("metadata.broker.list", brokers);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("request.required.acks", "1");
			// instance of producer configuration
			ProducerConfig config = new ProducerConfig(props);
			// instance of producer object
			Producer<String, String> producer = new Producer<String, String>(
					config);
			// create the topic with 4 partitions and 1 replica
			CreateTopic createTopic = new CreateTopic(zkConnectionString);
			createTopic.createTopic(topicName, 4, 1);
			// push 1000 records into kafka (if numberOfEvents=1000)
			for (long event = 0; event < numberOfEvents; event++) {
				//it contains two arguments: topic and data
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						topicName, "Hello World - " + event);
				// send the data to kafka
				producer.send(data);
				Thread.sleep(10);
				System.out.println("Produced event : "+ event);
			}
			producer.close();
		} catch (Exception e) {
			System.out.println("Producer failed " + e);
		}
	}

	public static void main(String[] args) {
		// takes numberOfEvents, brokers, topic and zookeepers details
		new KafkaProduce().produce(1000, "localhost:9092","demoTopic2", "localhost:2181");
	}

}
