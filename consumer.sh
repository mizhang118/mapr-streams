#!/bin/bash

java -cp `mapr classpath`:target/mapr-streams-examples-1.0-SNAPSHOT-jar-with-dependencies.jar com.mapr.examples.Run consumer $@