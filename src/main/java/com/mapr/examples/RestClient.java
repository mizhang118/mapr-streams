package com.mapr.examples;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

public class RestClient {
	public static void main(String[] args) throws Exception {
		
		String url = "https://127.0.0.1:8083/topics/%2Fstreams%2Ftest-stream%3Atest-topic";
		URL obj = new URL(url);
		HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

		//add reuqest header
		con.setRequestMethod("GET");
		con.setRequestProperty("content-type", "application/json");
		con.setRequestProperty("authorization", "Basic YWRtaW46cXVhaGJvaHlpZXZ1eWFlWmVEYWhmYTZhZWY0VG9idTdlYXZhaHdvbw==");
		con.setRequestProperty("cache-control", "no-cache");
		con.setRequestProperty("postman-token", "b266557d-4400-2c93-1781-3d15eba68ad2");

		//String urlParameters = "sn=C02G8416DRJM&cn=&locale=&caller=&num=12345";

		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		//wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		//System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		//print result
		System.out.println(response.toString());
	}

}
