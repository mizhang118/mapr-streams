package com.mapr.examples;

import com.mapr.udntest.MaprStreamsMock;
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
        
        MaprStreamsMock mock = new MaprStreamsMock();
        
        switch (args[0]) {
        	case "test":
        		mock.run();
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
