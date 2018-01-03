package com.mapr.udntest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;

public class MaprStreamsMock {
	protected String topic = "/streams/media-delivery.log:conductor_access_json.phx01";
	protected String input = "test.data";
	protected double testRate = 1;
	protected StringBuilder report = new StringBuilder();
	protected Producer producer = null;
	protected Consumer consumer = null;
	
	protected long waitingTime = 30000;			//milliseconds
	protected long startTime = System.currentTimeMillis();
	protected int testCaseNum = 0;
	protected int passCaseNum = 0;
	protected File reportDir = new File("report");
	protected PrintStream writer = null;
	
	public MaprStreamsMock() {
		init();
	}
	
	public MaprStreamsMock(String topic) {
		this();
		if ( topic != null ) {
			this.topic = topic;
		}
	}
	
	private void init() {
		try {
			FileInputStream props = new FileInputStream("./app.properties");
	        Properties properties = new Properties();
	        properties.load(props);
	        
	        String configTopic = properties.getProperty("topic");
	        if ( configTopic != null ) {
	        	this.topic = configTopic;
	        }
	        String configTestRate = properties.getProperty("testRate");
	        if ( configTestRate != null ) {
	        	try { this.testRate = Double.parseDouble(configTestRate); } catch (Exception e) { e.printStackTrace(System.err); }
	        }
	        String configTestData = properties.getProperty("testData");
	        if ( configTestData != null ) {
	        	this.input = configTestData;
	        }
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
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
		System.err.println("Create report  ...");
		String topicName = "topic";
		String[] f = topic.split(":");
		if ( f.length > 1 ) {
			topicName = f[1];
		}
		try { writer = new PrintStream(new FileOutputStream(new File(reportDir, topicName + "." + startTime))); } catch (Exception e) { e.printStackTrace(System.err); }
		if ( writer == null ) {
			writer = System.out;
		}
		
		System.err.println("Start MapR-Streams consumer ...");
		consumer = new Consumer(this.topic);
		consumer.setTestRate(testRate);
		Thread t = new Thread(consumer);
		t.start();
		
		System.err.println("Start MapR-Streams producer ...");
		producer = new Producer(topic);
		producer.setTestRate(testRate);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(input));
			
			String line = null;
			while ( (line = reader.readLine()) != null ) {
				line = line.trim();
				if ( line.length() == 0 ) {
					continue;
				}
				
				DataItem item = new DataItem(line);
				String li = item.toString();
				//System.err.println("********" + li);
				producer.produce(li);
			}
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		finally {
			try { reader.close(); } catch (Exception e) { e.printStackTrace(System.err); }
		}
		
		producer.flush();
		System.err.println("" + producer.getCount() + " items have been pushed into MapStreams topic: " + producer.getTopic());
		
		//DataItem.keys();
		
		producer.close();
	}
	
	public void waitingForTest() {
		System.err.println("Wait for " + (waitingTime/1000) + " seconds and then do testing ...");
		try { Thread.sleep(waitingTime); } catch (Exception e) { e.printStackTrace(System.err); }
	}
	
	public void test() {
		System.err.println("Start to do testing ...");
		System.err.println(consumer.getCount() + " items have been consumed from topic " + consumer.getTopic());
	}
	
	public String report() {
		System.err.println("***********Test Report************");
		System.err.println("Total cases: " + this.testCaseNum + ". Pass cases: " + this.passCaseNum + ". Faliure cases: " + (this.testCaseNum - this.passCaseNum));
		System.err.println("**********************************");
		return report.toString();
	}
	
	public void close() {
		try { consumer.close(); } catch (Exception e) {}
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
