package com.dataflair.kafka;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaEncoderDecoder {

	/**
	 * This produce the list of records into kafka using list encoder class.
	 */
	public void produce(long numberOfEvents, String brokers, String topicName,
			String zkConnectionString) {
		try {
			Properties props = new Properties();
			// 192.168.65.128:9092,192.168.65.129:9092
			props.put("metadata.broker.list", brokers);
			// Here we need to specify the encoder class
			// It will record data from list to byte[]
			props.put("serializer.class",
					"com.dataflair.kafka.KafkaListEncoder");
			// We also need to specify the key encoder. As for each record key is topic, which is by default string value.
			props.put("key.serializer.class", "kafka.serializer.StringEncoder");
			props.put("partitioner.class",
					"com.dataflair.kafka.RoundRobinPartitioner");
			// require ACK for each record
			props.put("request.required.acks", "1");

			ProducerConfig config = new ProducerConfig(props);
			Producer<String, List<Map<String, Object>>> producer = new Producer<String, List<Map<String, Object>>>(
					config);
			CreateTopic createTopic = new CreateTopic(zkConnectionString);
			createTopic.createTopic(topicName, 4, 1);
			List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();
			for (long event = 1; event <= numberOfEvents; event++) {
				Map<String, Object> record = new HashMap<String, Object>();
				record.put("data", "hello India");
				record.put("key", event);
				// prepare the list of 10 records and send in one go
				records.add(record);
				if (event % 10 == 0) {
					KeyedMessage<String, List<Map<String, Object>>> data = new KeyedMessage<String, List<Map<String, Object>>>(
							topicName, (event / 10) + "", records);
										producer.send(data);
					records.clear();
					Thread.sleep(10);
					System.out.println("Produced event : " + event / 10);
				}

			}
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Producer failed " + e);
		}
	}

	public static void main(String[] args) {
		new KafkaEncoderDecoder().produce(1000, "localhost:9092", "kafkasyncbatch",
				"localhost:2181");
	}

}
