package com.dataflair.kafka;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Round robin partitioner using a simple thread safe AotmicInteger
 */
public class RoundRobinPartitioner implements Partitioner {
    private static final Logger log = Logger.getLogger(RoundRobinPartitioner.class);

    final AtomicInteger counter = new AtomicInteger(0);

    public RoundRobinPartitioner(VerifiableProperties props) {
        log.trace("Instatiated the Round Robin Partitioner class");
    }
    /**
     * Take key as value and return the partition number
     */
    public int partition(Object key, int partitions) {

    	System.out.println("key" + key);
    	System.out.print("partition : " + partitions);
    	return (Integer.parseInt(key.toString())%4);
    }
}