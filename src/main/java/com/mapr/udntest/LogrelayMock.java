package com.mapr.udntest;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class LogrelayMock extends MaprStreamsMock {
	private long timetamp = System.currentTimeMillis();

	public LogrelayMock(String topic) {
		super(topic);
	}
	
	@Override
	public void test() {
		super.test();
		List<String> items = producer.getItems();
		System.err.println("" + items.size() + " Items are used for testing" );
		Map<String, String> testMap = new HashMap<String, String>();
		for ( String item : items ) {
			if ( testMap.get(item) != null ) {
				System.err.println("************Douplicate test record has been found: " + item);
			}
			else {
				testMap.put(item, "");
			}
		}
		
		
	}
}

