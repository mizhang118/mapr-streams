package com.mapr.udntest;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


/**
* Assumes mapr installed in /opt/mapr
*
* Compilation:
* javac -cp $(hadoop classpath) MapRTest.java
*
* Run:
* java -cp .:$(hadoop classpath) MapRTest /test
*/
public class MapRTest
{
	public static void main(String args[]) throws Exception {
        byte buf[] = new byte[ 65*1024];
        int ac = 0;
        if (args.length != 1) {
            System.out.println("usage: MapRTest pathname");
        return;
        }
/*
        maprfs:/// -> uses the first entry in /opt/mapr/conf/mapr-clusters.conf
        maprfs:///mapr/my.cluster.com/
        /mapr/my.cluster.com/

        String uri = "maprfs:///";
*/
        String dirname = args[ac++];

        Configuration conf = new Configuration();
        
        FileSystem fs = FileSystem.get(conf);
        
        Path dirpath = new Path( dirname + "/dir");
        Path wfilepath = new Path( dirname + "/file.w");
        Path rfilepath = wfilepath;

        boolean res = fs.mkdirs( dirpath);
        if (!res) {
                System.out.println("mkdir failed, path: " + dirpath);
        return;
        }

        System.out.println( "mkdir( " + dirpath + ") went ok, now writing file");

        FSDataOutputStream ostr = fs.create( wfilepath,
                true,
                512, // buffersize
                (short) 1, // replication
                (long)(64*1024*1024) // chunksize
                );
        ostr.write(buf);
        ostr.close();

        System.out.println( "write( " + wfilepath + ") went ok");

        System.out.println( "reading file: " + rfilepath);
        FSDataInputStream istr = fs.open( rfilepath);
        int bb = istr.readInt();
        istr.close();
        System.out.println( "Read ok");
        
        Path path = new Path("/ingest/media-delivery.log/conductor_access_raw/2017/12/21/00/00/logdata_mapr-logrelay-2-las01.cdx-test.unifieddeliverynetwork.net.1513814622746.gz");
    	SequenceFile.Reader reader = null;
    	try {		
    	reader = new SequenceFile.Reader(conf, Reader.file(path), Reader.bufferSize(4096), Reader.start(0));
    	Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    	Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
    	/*long position = reader.getPosition();
    	reader.seek(position);*/
    	int count = 0;
    	while (reader.next(key, value)) {
              String syncSeen = reader.syncSeen() ? "*" : "";
              System.out.printf("[%s]\t%s\t%s\n", syncSeen, key, value);
              
              if ( ++count > 2 )
            	  break;
    	}
    	} finally {
    		IOUtils.closeStream(reader);
    		}
    	
    	String pa = "/ingest/media-delivery.log/conductor_access_raw/2017/12/21/22/40";
    	System.out.println("list all files at " + pa);
    	Path pat = new Path( pa);
    	FileStatus[] fss = fs.listStatus(pat);
    	for ( FileStatus status : fss ) {
    		System.out.println("Dir? " + status.isDirectory() + " : " + status.getPath().getName());
    	}
    }
 
}
