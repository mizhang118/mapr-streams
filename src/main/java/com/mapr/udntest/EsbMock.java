package com.mapr.udntest;

import java.util.List;
import java.util.Map;

import org.json.JSONObject;

public class EsbMock extends MaprStreamsMock {

	public EsbMock(String topic) {
		super(topic);
	}
	
	@Override
	public void test() {
		super.test();
		
		Map<String, Integer> map = DataItem.esIndnices();
		System.err.println("" + map.size() + " indices are created in ElasticSearch. Test the existence and document number of each index...");
		
    	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    		String idx = entry.getKey();
    		Integer count = entry.getValue();
    		
    		long time = testCreateTime(idx);
    		int num = testDocumentNumber(idx);
    		
    		System.err.println("Take " + time + " milliseconds to create index " + idx + ".");
    		
			this.testCaseNum++;
    		if ( count == num ) {
    			System.err.println("Pass: index " + idx + " contains correct number of documents(" + num + ").");
    			this.passCaseNum++;
    		}
    		else {
    			System.err.println("Fail: index " + idx + " contains incorrect number of documents(" + num + ") while testing sends out " + count + " documents");
    		}
    	}
		
		List<String> items = producer.getItems();
		System.err.println("" + items.size() + " Items are used for testing" );
		
		
	}
	
	private long testCreateTime(String idx) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/" + idx);
		rest.setContentType("aplication/json");
		rest.setMethod("GET");
		rest.setAuth("esb:BtS4ueXz8kCySztF");

		JSONObject obj = rest.execJson();
		
		long time = -1;
		try { 
			long createDate = obj.getJSONObject(idx).getJSONObject("settings").getJSONObject("index").getLong("creation_date");
			time = createDate = this.startTime;
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		
		return time;
	}
	
	private int testDocumentNumber(String idx) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/" + idx + "/_stats");
		rest.setContentType("aplication/json");
		rest.setMethod("GET");
		rest.setAuth("esb:BtS4ueXz8kCySztF");

		JSONObject obj = rest.execJson();
		
		int docsCount = -1;
		try { 
			docsCount = obj.getJSONObject("indices").getJSONObject(idx).getJSONObject("primaries").getJSONObject("docs").getInt("count");
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
		}
		
		return docsCount;
	}
}
