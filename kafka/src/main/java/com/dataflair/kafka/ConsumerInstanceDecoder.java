package com.dataflair.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
/**
 * This class recieved the byte[] data and call the decode class to covert into list
 * This is the one consumer instance in a group
 * @author centos
 *
 */
public class ConsumerInstanceDecoder implements Runnable {
		private KafkaStream stream;
		private int threadNumber;
		private ConsumerConnector consumer;
		private KafkaMessageDecoder kafkaDecoder = new KafkaMessageDecoder();
		public ConsumerInstanceDecoder(KafkaStream stream, int threadNumber, ConsumerConnector consumer) {
			this.threadNumber = threadNumber;
			this.stream = stream;
			this.consumer =consumer;
		}

		public void run() {
			// Iterate over the stream
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
					// print list on console
					System.out.println("Thread " + threadNumber + ": "
						+ new KafkaMessageDecoder().decode(it.next().message()));
			}
			System.out.println("Shutting down Thread: " + threadNumber);
		}
}
