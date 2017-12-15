package com.mapr.udntest;

import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.io.Resources;

public class Consumer extends Thread {
	private String groupId = "mapr-test";
	private String topic = null;
	private long consumeSize = 1000;
	private long skipSize = 0;
	private double testRate = 1;
	private boolean done = false;

	private KafkaConsumer<String, String> consumer = null;
	private List<String> items = new ArrayList<String>();
	
	public Consumer(String topic) {
		this.topic = topic;
	}
	
	public Consumer(String topic, String groupId, long cSize, double tRate) {
		this.topic = topic;
		this.groupId = groupId;
		this.consumeSize = cSize;
		this.testRate = tRate;
	}
	
	public void consume() throws IOException {
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            properties.setProperty("group.id", groupId);

            consumer = new KafkaConsumer<>(properties);
        }	
        
        consumer.subscribe(Arrays.asList(topic));
        int timeouts = 0;
        int count = 0;
        while ( count < consumeSize ) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.err.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
            	
            	if ( Math.random() <= testRate ) {
            		items.add(record.value());
            	}
            	else {
            		skipSize++;
            	}
            }
            
            consumer.commitAsync();
        }
	}
	
	public boolean isDone() {
		return done;
	}
	
	public void close() {
		try { consumer.commitAsync(); consumer.close(); Thread.sleep(1000); } catch (Exception e) { e.printStackTrace(System.err); }
		if ( !done ) {
			try { this.interrupt(); } catch (Exception e) { e.printStackTrace(System.err); }
		}
	}
	
	public List<String> getItems() {
		return this.items;
	}
	
	public long getItemSize() {
		return items.size();
	}
	
	@Override
	public void run() {
		try {
			this.consume();
		}
		catch(Exception e) {
			e.printStackTrace(System.err);
		}
		finally {
			done = true;
		}
	}
}
