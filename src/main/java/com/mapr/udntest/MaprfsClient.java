package com.mapr.udntest;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class MaprfsClient {
	public static final String MEDIA_DELIVERY_LOG_BASEDIR = "/ingest/media-delivery.log/conductor_access_raw";
	protected FileSystem fs = null;
	protected Configuration conf = new Configuration();
	
	public MaprfsClient() {
		try {
			fs = FileSystem.get(conf);
		}
        catch(Exception e) {
        	e.printStackTrace(System.err);
        }
	}
	
	public List<String> readSequenceFile(String file) {
		List<String> list = new ArrayList<String>();
		
        Path path = new Path(file);
    	SequenceFile.Reader reader = null;
    	try {		
    		reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
    		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    		/*long position = reader.getPosition();
    		reader.seek(position);*/
    		int count = 0;
    		while (reader.next(key, value)) {
    			//String syncSeen = reader.syncSeen() ? "*" : "";
    			//System.out.printf("[%s]\t%s\t%s\n", syncSeen, key, value);
    			list.add(key + "\t" + value);
    			count++;
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace(System.err);
    	}
    	finally {
    		IOUtils.closeStream(reader);
    	}		
		return list;
	}
	
	public List<String> listFiles(String dir) {
		List<String> list = new ArrayList<String>();
		
		try {
			Path path = new Path(dir);
			FileStatus[] fileStates = fs.listStatus(path);
			for ( FileStatus status : fileStates ) {
				list.add(status.getPath().getName());
				//System.out.println("Dir? " + status.isDirectory() + " : " + status.getPath().getName());
			}
		}
    	catch(Exception e) {
    		e.printStackTrace(System.err);
    	}
    	
		return list;
	}
	
	private int[] utc(long timestamp) {
		SimpleDateFormat utc = new SimpleDateFormat("yyyy/MM/dd/HH/mm/ss");
		utc.setTimeZone(TimeZone.getTimeZone("UTC"));
		String str = utc.format(new Date(timestamp));
		String s[] = str.split("/");
		int[] time = new int[s.length];
		for( int i = 0; i < s.length; i++ ) {
			time[i] = Integer.parseInt(s[i]);
		}
		
		return time;
	}
	
	public String dataDir(long timestamp) {
		int[] arr = utc(timestamp);
		int mm = arr[4];
		arr[4] = 5 * (mm / 5);
		
		return this.MEDIA_DELIVERY_LOG_BASEDIR + "/" + arr[0] + "/" + String.format("%02d", arr[1]) + "/" + String.format("%02d", arr[2]) + "/" + String.format("%02d", arr[3]) + "/" + String.format("%02d", arr[4]);
	}
	
	public String nextDataDir(long timestamp) {
		return dataDir(timestamp + 5 * 60 * 1000);
	}
	
	public static void main(String[] args) {
		MaprfsClient client = new MaprfsClient();
		long ts = System.currentTimeMillis();
		System.out.println(client.dataDir(ts));
		System.out.println(client.nextDataDir(ts));
		
		
	}

}
