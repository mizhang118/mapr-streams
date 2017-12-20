package com.mapr.udntest;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;

public class EsbMock extends MaprStreamsMock {

	public EsbMock(String topic) {
		super(topic);
	}
	
	@Override
	public void test() {
		super.test();
		
		List<String> items = producer.getItems();
		System.err.println("" + items.size() + " Items are used for testing" );
		Map<String, JSONObject> testMap = new HashMap<String, JSONObject>();
		for ( String item : items ) {
			try {
				JSONObject obj = new JSONObject(item);
				String esId = obj.getString("es-id");
				if ( testMap.get(esId) != null ) {
					System.err.println("************Douplicate es-id has been found: " + esId);
				}
				else {
					testMap.put(esId, obj);
				}
			}
			catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}
		
		Map<String, Integer> map = DataItem.esIndnices();
		System.err.println("" + map.size() + " indices are created in ElasticSearch. Test the existence and document number of each index...");
		
    	for (Map.Entry<String, Integer> entry : map.entrySet()) {
    		String idx = entry.getKey();
    		Integer count = entry.getValue();
    		
    		long time = testCreateTime(idx);
    		int num = testDocumentNumber(idx);
    		this.testCaseNum++;
    		if ( time > 0 ) {
    			this.passCaseNum++;
    			writer.println("Pass: " + " index " + idx + " was created in " + time + " milliseconds.");
    		}
    		else {
    			writer.println("Fail: " + " index " + idx + " was not created.");
    		}
    		
			this.testCaseNum++;
    		if ( count == num ) {
    			writer.println("Pass: index " + idx + " contains correct number of documents(" + num + ").");
    			this.passCaseNum++;
    		}
    		else {
    			writer.println("Fail: index " + idx + " contains incorrect number of documents(" + num + ") while testing sends out " + count + " documents");
    		}
    		
    		this.testItems(idx, testMap, count + 1);
    		
    		writer.flush();
    	}
		
		
	}
	
	private long testCreateTime(String idx) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/" + idx);
		rest.setContentType("aplication/json");
		rest.setMethod("GET");
		rest.setAuth("esb:BtS4ueXz8kCySztF");

		JSONObject obj = rest.execJson();
		//System.out.println("(" + idx + ")" + "=(" + obj + ")");
		
		long time = -1;
		try { 
			long createDate = obj.getJSONObject(idx).getJSONObject("settings").getJSONObject("index").getLong("creation_date");
			time = createDate - this.startTime;
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
	
	private int testItems(String idx, Map<String, JSONObject> map, int size) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/" + idx + "/_search?size=" + size);
		rest.setContentType("aplication/json");
		rest.setMethod("GET");
		rest.setAuth("esb:BtS4ueXz8kCySztF");

		JSONObject obj = rest.execJson();
		
		int docsCount = 0;
		try {
			JSONArray array = obj.getJSONObject("hits").getJSONArray("hits");
			for ( int i = 0; i < array.length(); i++ ) {
				this.testCaseNum++;
				String esId = array.getJSONObject(i).getString("_id");
				JSONObject hit = array.getJSONObject(i).getJSONObject("_source");
				JSONObject test = map.get(esId);
				test.remove("es-id");
				test.remove("es-type");
				test.remove("es-index");
				if ( test != null && test.toString().equals(hit.toString()) ) {
					this.passCaseNum++;
					writer.println("Pass: " + "JSON item with es-id " + esId + " are equal.");
				}
				else {
					writer.println("Fail: " + "JSON item with es-id " + esId + " are not equal.");
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace(System.err);
			writer.println("Fail: " + " with exception " + e.getMessage());
		}
		
		return docsCount;
	}
}
