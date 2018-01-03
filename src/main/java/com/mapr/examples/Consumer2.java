package com.mapr.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are analyzed
 * to estimate latency (assuming clock synchronization between producer and consumer).
 * <p/>
 * Whenever a message is received on "slow-messages", the stats are dumped.
 */
public class Consumer2 {
	private static final String DEFAULT_GROUP_ID = "mapr-test";
	
    public static void main(String[] args) throws IOException {
    	//read in app parameters
    	String topic = null;
    	String groupId = null;
    	
    	if ( args.length <= 1 ) {
    		System.err.println("Please enter mapr-stream topic (e.g. /path:topic-name) \nand groupId (optional, default as mapr-test) as parameters");
    		return;
    	}
    	
    	if ( args.length > 1 ) {
    		topic = args[1];
    	}
    	
    	if ( args.length > 2 ) {
    		groupId = args[2];
    	}
        
        if ( groupId == null ) {
        	groupId = DEFAULT_GROUP_ID;
        }

        // and the consumer
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.setProperty("group.id", groupId);

            consumer = new KafkaConsumer<>(properties);
        }
        consumer.subscribe(Arrays.asList(topic));
        int timeouts = 0;
        //noinspection InfiniteLoopStatement
        while (true) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.err.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
            	System.out.printf("partition: %d; offset: %d; key: %s; value: %s\n", record.partition(), record.offset(), record.key(), record.value());
            }
            
            consumer.commitAsync();
        }
        
        //try { consumer.close(); } catch (Exception e) {}
    }
}
