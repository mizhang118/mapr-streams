package com.mapr.udntest;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class DataItem {
	private String input = null;
	private String output = null;
	
	private static Map<String, Integer> indices = new HashMap<String, Integer>();
	
	public DataItem(String input) {
		this.input = input;
		convert();
	}
	
	private void convert() {
		if ( input == null ) {
			return;
		}
		
        JSONObject jobj = null;
        
        
        try {
        	jobj = new JSONObject(input);       	
        	output = convertJson(jobj);
        }
        catch(Exception e) {
        	output = convertString(input);
        }
	}
	
	private String convertJson(JSONObject obj) {
		String idx = obj.getString("es-index");
		idx = changeEsIndex(idx.toString());
		obj.put("es-index", idx);
		
		if ( idx != null ) {
			Integer count = indices.get(idx);
			if ( count == null ) {
				cleanIndex(idx);
				indices.put(idx, 1);
			}
			else
				indices.put(idx, ++count);
		}
		
		return obj.toString();
	}
	
	private String convertString(String in) {
		
		
		
		return in;
	}
	
	private String changeEsIndex(String esIndex) {
		String test = "-2019";
		String test1 = "-2017";
		String test2 = "-2018";
		String changed = esIndex;
		
		if ( esIndex.indexOf(test1) > 0 ) {
			changed = esIndex.replaceFirst(test1, test);
		}
		else if ( esIndex.indexOf(test2) > 0 ) {
			changed = esIndex.replaceFirst(test2, test);
		}
		
		return changed;
	}
	
	public void cleanIndex(String idx) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/" + idx);
		rest.setMethod("DELETE");
		rest.setAuth("esb:BtS4ueXz8kCySztF");
		System.err.println("Delete index: " + rest.exec());
	}
	
	@Override
	public String toString() {
		return this.output;
	}
	
	public static void keys() {
		System.err.println("" + indices.size() + " unique keys are in the data");
    	for (Map.Entry<String, Integer> entry : indices.entrySet()) {
    		System.err.println("\nvvvvvvvvvvvvvvvvvvvv\n"+entry.getKey() + "[" + entry.getKey().getClass() + "]=" + entry.getValue() + "\n^^^^^^^^^^^^^^^^^^^");
    	}
	}
	
	public static Map<String, Integer> esIndnices() {
		return indices;
	}
	
}
