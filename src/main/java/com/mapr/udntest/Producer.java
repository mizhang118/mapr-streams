package com.mapr.udntest;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class Producer {
	private String topic = null;
	private KafkaProducer<String, String> producer = null;
	private long count = 0;
	private long skipSize = 0;
	private double testRate = 1;
	private boolean done = false;

	private List<String> items = new ArrayList<String>();
	
	public Producer(String topic) {
		this.topic = topic;
		try { init(); } catch (Exception e) { e.printStackTrace(System.err); }
	}
	
	private void init() throws IOException {
        // set up the producer
		FileInputStream props = new FileInputStream("./producer.properties");
        Properties properties = new Properties();
        properties.load(props);
        producer = new KafkaProducer<>(properties);	
	}

	public void produce(String data) {
		producer.send(new ProducerRecord<String, String>(topic, Consumer.MESSAGE_KEY, data));
		count++;
		if ( count % 100 == 18 ) {
			producer.flush();
		}
		
		if ( Math.random() <= testRate ) {
			items.add(data);
		}
		else {
			skipSize++;
		}
	}
	
	public void flush() {
		producer.flush();
	}
	
	public void close() {
		try { producer.flush(); producer.close(); } catch (Exception e) { e.printStackTrace(System.err);}
		done = true;
	}
	
	public long getCount() {
		return count;
	}
	
	public List<String> getItems() {
		return this.items;
	}
}
