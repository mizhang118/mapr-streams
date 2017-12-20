package com.mapr.udntest;

import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.io.OutputStreamWriter;
import java.net.URL;

import org.json.JSONObject;
 
public class RestClient {
	private String url = null;
	private HttpURLConnection conn = null;
	private String contentType = "application/json";
	private String userPassword = "esb:BtS4ueXz8kCySztF";
	private String method = "GET";
	private String data = null;
	
	public RestClient(String url) {
		this.url = url;
		connect();
	}
	
	private HttpURLConnection connect() {
	    try {
	    	 
	        URL obj = new URL(url);
	        conn = (HttpURLConnection) obj.openConnection();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    
		return conn;
	}
	
	public void setContentType(String ct) {
		if ( ct != null )
			this.contentType = ct;
		
		this.conn.setRequestProperty("Content-Type", contentType);
	}
	
	public void setAuth(String userPassword) {
		if ( userPassword != null )
			this.userPassword = userPassword;
		
		try { 
			String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(this.userPassword.getBytes("UTF-8"));
			conn.setRequestProperty ("Authorization", basicAuth);
		} 
		catch (Exception e) { 
			e.printStackTrace(System.err); 
		}
	}
	
	public void setMethod(String method) {
		if ( method != null )
			this.method = method;
	}
	
	public void setData(String data) {
		this.data = data;
	}
	
	public String exec() {
		StringBuilder builder = new StringBuilder();
		try {
			conn.setRequestMethod(method);
			if ( data != null ) {
				OutputStreamWriter out = new OutputStreamWriter(conn.getOutputStream());
				out.write(data);
				out.close();
			}
			
			InputStreamReader reader = new InputStreamReader(conn.getInputStream());
			
			int c = 0;
			while ( (c=reader.read()) != -1 ) {
				builder.append((char) c);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return builder.toString();
	}
	
	public JSONObject execJson() {
		String ret = exec();
		//System.err.println(">>>>>URL:" + url);
		//System.err.println(">>>>>Result:" + ret);
		JSONObject obj = null; 
		try { obj = new JSONObject(ret); } catch (Exception e) { e.printStackTrace(); }
		
		return obj;
	}
 
	public static void main(String[] args) {
		RestClient rest = new RestClient("https://elastic1-sea.cdx-test.unifieddeliverynetwork.net:9200/kafka-access-log-lines-2019.11.19/_search?size=100");
		rest.setContentType(null);
		rest.setAuth(null);
		System.out.println(rest.exec());
	}
}
