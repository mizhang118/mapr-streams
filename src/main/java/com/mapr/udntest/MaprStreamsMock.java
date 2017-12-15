package com.mapr.udntest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

public class MaprStreamsMock {
	private StringBuilder report = new StringBuilder();
	private Consumer consumer = null;

	public void prepare() {
		System.err.println("Start MapR-Streams consumer ...");
		consumer = new Consumer("/streams/test-stream:test-topic");
		consumer.start();
		
		System.err.println("Start MapR-Streams producer ...");
		Producer producer = new Producer("/streams/test-stream:test-topic");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader("test.data"));
			
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
}
