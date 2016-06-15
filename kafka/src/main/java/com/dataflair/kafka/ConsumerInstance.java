package com.dataflair.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
/**
 * This class recieved the byte[] data and convert into string and print on console
 * This is the one consumer instance in a group
 * @author centos
 *
 */
public class ConsumerInstance implements Runnable {
		private KafkaStream stream;
		private int threadNumber;
		private ConsumerConnector consumer;
		public ConsumerInstance(KafkaStream stream, int threadNumber, ConsumerConnector consumer) {
			this.threadNumber = threadNumber;
			this.stream = stream;
			this.consumer =consumer;
		}

		public void run() {

			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			// Iterate over the list
			while (it.hasNext()) {
					// print the data on console
					System.out.println("Thread " + threadNumber + ": "
						+ new String(it.next().message()));
			}
			System.out.println("Shutting down Thread: " + threadNumber);
		}
}
