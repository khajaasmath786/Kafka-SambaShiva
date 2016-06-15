package com.dataflair.kafka;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaAyncProduce {

	/**
	 *
	 *
	 */
	public void produce(long numberOfEvents, String brokers, String topicName, String zkConnectionString) {
		try {
			Properties props = new Properties();
			// 192.168.65.128:9092,192.168.65.129:9092
			props.put("metadata.broker.list", brokers);
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("partitioner.class", "com.dataflair.kafka.RoundRobinPartitioner");
			// It is set as async if you want to use async mode
			props.put("producer.type","async");
			props.put("queue.buffering.max.ms","5000");
			props.put("queue.buffering.max.messages","10000");

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, String> producer = new Producer<String, String>(
					config);
			CreateTopic createTopic = new CreateTopic(zkConnectionString);
			createTopic.createTopic(topicName, 4, 1);
			for (long event = 0; event < numberOfEvents; event++) {
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(
						topicName, event + "", "Hello World - " + event);
				// Hello World
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
		new KafkaAyncProduce().produce(1000, "localhost:9092","demoasync", "localhost:2181");
	}

}
