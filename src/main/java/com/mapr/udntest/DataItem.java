package com.mapr.udntest;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class DataItem {
	private String input = null;
	private String output = null;
	
	public DataItem(String input) {
		this.input = input;
		convert();
	}
	
	private void convert() {
		if ( input == null ) {
			return;
		}
		
        ObjectMapper mapper = new ObjectMapper();
        
        try {
        	Map<?,?> map = mapper.readValue(input, Map.class);
        	for (Map.Entry<?, ?> entry : map.entrySet()) {
        		System.err.println("\n----------------------------\n"+entry.getKey() + "[" + entry.getKey().getClass() + "]=" + entry.getValue() + "[" + entry.getValue().getClass() + "]\n");
        	}
        	
        	output = convertJson(map, mapper);
        }
        catch(Exception e) {
        	output = convertString(input);
        }
	}
	
	private String convertJson(Map<?,?> map, ObjectMapper mapper) {
		String str = null;
		try {
			str = mapper.writeValueAsString(map);
		}
		catch(Exception e) {
			e.printStackTrace(System.err);
		}
		
		return str;
	}
	
	private String convertString(String in) {
		
		
		
		return in;
	}
	
	@Override
	public String toString() {
		return this.output;
	}
}
