package com.mapr.udntest;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class LogrelayMock extends MaprStreamsMock {
	private long startTimetamp = System.currentTimeMillis();
	private long endTimetamp = System.currentTimeMillis();

	public LogrelayMock(String topic) {
		super(topic);
		
		super.waitingTime = 5 * 60 * 1000 + 5000; //waiting time is 5 min and 5 seconds
	}
	
	@Override
	public void test() {
		super.test();
		
		//push data into corresponding mapr-streams topic
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
		
		//after wating for 5 mins 5 seconds, check the data in mapr-fs
		System.err.println("Check MapR-FS and verify if the data have been landed onto MapR-FS.");
		System.err.println("Check MapR-FS file has been generated on timestamp " + this.startTimetamp);
		
		//list mapr-fs files
		MaprfsClient client = new MaprfsClient();
		String dir = client.dataDir(this.startTime);
		System.err.println("Check if data dir(" + dir + ") has been created in MapR-FS and list all files inside this directory.");
		List<String> files = client.listFiles(dir);
		System.err.println("Files are found: " + files);
		
		//check the content of each file
		System.err.println("Check the content of each MapR-FS file to verify if its content is same to the data pushed into MapR-Streams topic.");
		int[] check1 = testFileContent(client, dir, files, testMap);
		
		System.err.println("There are " + check1[0] + " data records are found in the MapR-FS dir " + dir);
		System.err.println("There are " + check1[1] + " data records are matched in this dir.");
		
		//matched items are not enough so that we check next folder (5 min later)
		int[] check2 = null;
		if ( check1[1] < items.size() ) {
			String dir2 = client.nextDataDir(this.startTime);
			List<String> files2 = client.listFiles(dir);
			check2 = testFileContent(client, dir2, files2, testMap);
			System.err.println("There are " + check2[0] + " data records are found in the MapR-FS dir " + dir2);
			System.err.println("There are " + check2[1] + " data records are matched in this dir.");
		}
		
		int totalMatched = check1[0];
		if ( check2 != null ) {
			totalMatched += check2[1];
		}
		
		if ( totalMatched == items.size() ) {
			System.err.println("Passed! Total " + items.size() + " items were submitted for testing and were verified in MapR-FS.");
		}
		else {
			System.err.println("Failed! Total " + items.size() + " items were submitted for testing, but " + totalMatched + " items were verified in MapR-FS.");
		}
	}
	
	private int[] testFileContent(MaprfsClient client, String dir, List<String> files, Map<String, String> map) {
		int count = 0;
		int matched = 0;
		for ( String file : files ) {
			String path = dir + "/" + file;
			System.err.println("Check content of MapR-FS file " + path);
			
			List<String> lines = client.readSequenceFile(path);
			count += lines.size();
			System.err.println("There are " + lines.size() + " data records are in " + path);
			
			for ( String line : lines ) {
				String[] str = line.split("\t");
				if ( str.length >= 2 &&  map.get(str[1]) != null ) {
					matched++;
				}
			}
		}
		
		int[] ret = new int[2];
		ret[0] = count;
		ret[1] = matched;
		
		return ret;
	}
}

