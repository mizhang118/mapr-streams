package com.mapr.examples;

import com.mapr.udntest.MaprStreamsMock;
import com.mapr.udntest.EsbMock;
import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer', 'consumer' or 'dbconsumer' as argument");
        }
        String test = args[0];
        
    	String p1 = null;
    	String p2 = null;
    	
    	if ( args.length > 1 ) {
    		p1 = args[1];
    	}
    	
    	if ( args.length > 2 ) {
    		p2 = args[2];
    	}
        
        MaprStreamsMock mock = new MaprStreamsMock(p1);
        if ( test.equals("esb") ) {
        	mock = new EsbMock(p1);
        }
        
        switch (test) {
        	case "test":
        		mock.run();
        		break;
        	case "esb":
        		mock.run();
        		break;
        	case "producer":
        		mock.produce(p2);
        		break;
        	case "consumer":
        		mock.consume(p2);
        		break;
        	case "producer2":
                Producer2.main(args);
                break;
            case "consumer2":
                Consumer2.main(args);
                break;
            case "dbconsumer":
                DBConsumer.main(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}
