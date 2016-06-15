package com.dataflair.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaConsumerDecoder {
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public KafkaConsumerDecoder(String zookeeper, String groupName, String topicName) {
		// Create the consumer configuration
		ConsumerConfig consumerConfig = createConsumerConfiguration(
				zookeeper, groupName);
		// Create the instance of Java ConsumerConnerctor class.
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(consumerConfig);
		// set the topic name
		this.topic = topicName;
	}

	public void shutdown() {
		if (consumer != null)
			// Shutdown all the running consumer instances
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public void run(int numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		// Set the topic and number of thread you want to use for read the data
		topicCountMap.put(topic, new Integer(numThreads));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

		// Get the stream for each topic
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		// now launch all the threads
		executor = Executors.newFixedThreadPool(numThreads);

		// now create an object to consume the messages
		int threadNumber = 0;
		for (final KafkaStream stream : streams) {
			// Create the instance of consumer by assigning stream to each instance.
			executor.submit(new ConsumerInstanceDecoder(stream, threadNumber,consumer));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfiguration(
			String zookeeper, String groupId) {
		Properties props = new Properties();
		// Set the IP and Port of zookeeper
		props.put("zookeeper.connect", zookeeper);
		// Set the Consumer group name.
		props.put("group.id", groupId);
		// Session timeout with zookeeper
		props.put("zookeeper.session.timeout.ms", "4000");
		// Sync time with zookeeper
		props.put("zookeeper.sync.time.ms", "2000");
		// Set the auto commit interval of offset
		props.put("auto.commit.interval.ms", "1000");
		// return the instance of ConsumerConfig() class.
		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {

		String zooKeeper = "localhost:2181";

		// Number of instance we want to run inside a group
		int threads = Integer.parseInt("4");
		// Create the instance of KafkaConsumer class
		KafkaConsumer kafkaExample = new KafkaConsumer(zooKeeper, "groupId",
				"topic");

		kafkaExample.run(threads);

		/*try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {
			System.out.println("Exception : " + ie.getMessage());
		}
		kafkaExample.shutdown();*/
	}
}
