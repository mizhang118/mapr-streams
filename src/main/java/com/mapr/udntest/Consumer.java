package com.mapr.udntest;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer extends Config implements Runnable {
	public static String MESSAGE_KEY = "UDNTEST_MESSAGE_KEY";
	
	private String groupId = "mapr-test";
	private long consumeSize = Long.MAX_VALUE;
	private boolean print = false;

	private KafkaConsumer<String, String> consumer = null;
	private List<String> items = new ArrayList<String>();
	
	public Consumer(String topic) {
		super();
		this.topic = topic;
	}

	public Consumer(String topic, String groupId) {
		this(topic);
		this.groupId = groupId;
	}
	
	public Consumer(String topic, String groupId, long cSize, double tRate) {
		this(topic);
		this.groupId = groupId;
		this.consumeSize = cSize;
		this.testRate = tRate;
	}
	
	public void consume() throws IOException {
        //InputStream props = Resources.getResource("consumer.properties").openStream())
		FileInputStream props = new FileInputStream("./consumer.properties");
        Properties properties = new Properties();
        properties.load(props);
        properties.setProperty("group.id", groupId);

        consumer = new KafkaConsumer<>(properties);
        
        consumer.subscribe(Arrays.asList(topic));
        int timeouts = 0;
        while ( count < consumeSize ) {
            // read records with a short timeout. If we time out, we don't really care.
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
                //System.err.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }
            for (ConsumerRecord<String, String> record : records) {
            	String key = record.key();
            	String value = record.value();
            	
            	if ( key != null && key.equals(MESSAGE_KEY) ) {
            		if ( print ) {
            			System.out.println(value);
            		}
            	
            		count++;
            		if ( Math.random() <= testRate ) {
            			//items.add(value);
            		}
            		else {
            			skipSize++;
            		}
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
			//try { this.interrupt(); } catch (Exception e) { e.printStackTrace(System.err); }
		}
	}
	
	public List<String> getItems() {
		return this.items;
	}
	
	public long getCount() {
		return this.count;
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
	
	public void setPrint(boolean p) {
		this.print = p;
	}
}
