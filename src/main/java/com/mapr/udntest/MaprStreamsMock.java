package com.mapr.udntest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class MaprStreamsMock {
	private String topic = "/streams/test-stream:test-topic";
	private String input = "test.data";
	private StringBuilder report = new StringBuilder();
	private Producer producer = null;
	private Consumer consumer = null;
	
	public MaprStreamsMock() {}
	
	public MaprStreamsMock(String topic) {
		if ( topic != null ) {
			this.topic = topic;
		}
	}
	
	public void run() {
		this.prepare();
		this.waitingForTest();
		this.test();
		this.report();
		this.close();
	}

	public void prepare() {
		System.err.println("Start MapR-Streams consumer ...");
		consumer = new Consumer(this.topic);
		consumer.start();
		
		System.err.println("Start MapR-Streams producer ...");
		producer = new Producer(topic);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			
			String line = null;
			while ( (line = reader.readLine()) != null ) {
				line = line.trim();
				if ( line.length() == 0 ) {
					continue;
				}
				
				producer.produce(line);
			}
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		finally {
			try { reader.close(); } catch (Exception e) { e.printStackTrace(System.err); }
		}
		
		producer.flush();
		System.err.println("" + producer.getCount() + " items have been pushed into MapStreams.");
		producer.close();
	}
	
	public void waitingForTest() {
		long sec = 10000;
		System.err.println("Wait for " + (sec/1000) + " seconds and then do testing ...");
		try { Thread.sleep(sec); } catch (Exception e) { e.printStackTrace(System.err); }
	}
	
	public void test() {
		System.err.println("Start to do testing ...");
		List<String> items = consumer.getItems();
		System.err.println("" + items.size() + " have been retrieved from MapR-Streams." );
		
		for ( String s : items ) {
			System.out.println(items);
		}
	}
	
	public String report() {
		return report.toString();
	}
	
	public void close() {
		consumer.close();
	}
	
	public void produce(String file) {
		if ( file != null ) {
			this.input = file;
		}
		
		Producer producer = new Producer(topic);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			
			String line = null;
			while ( (line = reader.readLine()) != null ) {
				line = line.trim();
				if ( line.length() == 0 ) {
					continue;
				}
				
				producer.produce(line);
			}
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		finally {
			try { reader.close(); } catch (Exception e) { e.printStackTrace(System.err); }
		}
		
		producer.flush();
		System.err.println("" + producer.getCount() + " items have been pushed into MapStreams.");
		producer.close();
		
		System.err.println("" + producer.getCount() + " data items have been sent to MapR-Streams topic " + this.topic + ".");
		System.err.println("" + producer.getItems().size() + " data items have been seleted for testing.");
	}
	
	public void consume(String groupId) {
		if ( groupId == null)
			consumer = new Consumer(this.topic);
		else
			consumer = new Consumer(this.topic, groupId);
		
		consumer.setPrint(true);
		
		try {
			consumer.consume();
		}
		catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
}
