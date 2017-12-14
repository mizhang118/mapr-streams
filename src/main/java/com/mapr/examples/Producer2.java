package com.mapr.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This producer will send a bunch of messages to topic "fast-messages". Every so often,
 * it will send a message to "slow-messages". This shows how messages can be sent to
 * multiple topics. On the receiving end, we will see both kinds of messages but will
 * also see how the two topics aren't really synchronized.
 */
public class Producer2 {
	private static final String DEFAULT_DATA_FILE = "./data.txt";
	
    public static void main(String[] args) throws IOException {
    	//read in app parameters
    	String topic = null;
    	String data = null;
    	
    	if ( args.length == 0 ) {
    		System.err.println("Please enter mapr-stream topic (e.g. /path:topic-name) and data file path (optional, default ./data.txt) as parameters");
    		return;
    	}
    	
    	if ( args.length > 0 ) {
    		topic = args[0];
    	}
    	
    	if ( args.length > 1 ) {
    		data = args[1];
    	}
        
        if ( data == null ) {
        	data = DEFAULT_DATA_FILE;
        }
        
        BufferedReader reader = new BufferedReader(new FileReader(data));

        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }
      
      try {
    	  String line = null;
    	  long count = 0;
    	  while ( (line = reader.readLine()) != null ) {
    		  producer.send(new ProducerRecord<String, String>(topic, line));
    		  count++;
              // every so often send to a different topic
              if (count % 1000 == 0) {
            	  producer.flush();
            	  System.err.println("Sent msg number %d\n" + count);
              }
    	  }
      } catch (Throwable throwable) {
    	  System.err.printf("%s", throwable.getStackTrace());
      } finally {
    	  producer.close();
      }
    }
}
