package com.mapr.udntest;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.io.Resources;

public class Producer {
	private String topic = null;
	private KafkaProducer<String, String> producer = null;
	private long count = 0;
	
	public Producer(String topic) {
		this.topic = topic;
		try { init(); } catch (Exception e) { e.printStackTrace(System.err); }
	}
	
	private void init() throws IOException {
        // set up the producer
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }		
	}

	public void produce(String data) {
		producer.send(new ProducerRecord<String, String>(topic, data));
		count++;
		if ( count % 100 == 18 ) {
			producer.flush();
		}
	}
	
	public void flush() {
		producer.flush();
	}
	
	public void close() {
		try { producer.flush(); producer.close(); } catch (Exception e) { e.printStackTrace(System.err);}
	}
	
	public long getCount() {
		return count;
	}
}
