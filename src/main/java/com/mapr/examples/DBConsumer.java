package com.mapr.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.ojai.Document;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * This program reads messages from two topics. Messages on "fast-messages" are analyzed
 * to estimate latency (assuming clock synchronization between producer and consumer).
 * <p/>
 * Each message is saved in a MapR-DB JSON table
 *
 * Note: this class is a copy of the Consumer class, with some changes to save data into MapR-DB JSON
 *
 */
public class DBConsumer {

  public static void main(String[] args) throws IOException {
    // set up house-keeping
    ObjectMapper mapper = new ObjectMapper();
    Histogram stats = new Histogram(1, 10000000, 2);
    Histogram global = new Histogram(1, 10000000, 2);

    final String TOPIC_FAST_MESSAGES = "/sample-stream:fast-messages";
    final String TOPIC_SUMMARY_MARKERS = "/sample-stream:summary-markers";


    Table fastMessagesTable = getTable("/apps/fast-messages");

    // and the consumer
    KafkaConsumer<String, String> consumer;
    try (InputStream props = Resources.getResource("consumer.props").openStream()) {
      Properties properties = new Properties();
      properties.load(props);
      // use a new group id for the dbconsumer
      if (properties.getProperty("group.id") == null) {
        properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
      } else {
        String groupId = properties.getProperty("group.id");
        properties.setProperty("group.id", "db-" + groupId);
      }

      consumer = new KafkaConsumer<>(properties);
    }
    consumer.subscribe(Arrays.asList(TOPIC_FAST_MESSAGES, TOPIC_SUMMARY_MARKERS));
    int timeouts = 0;

    //noinspection InfiniteLoopStatement
    while (true) {
      // read records with a short timeout. If we time out, we don't really care.
      ConsumerRecords<String, String> records = consumer.poll(200);
      if (records.count() == 0) {
        timeouts++;
      } else {
        System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
        timeouts = 0;
      }
      for (ConsumerRecord<String, String> record : records) {
        switch (record.topic()) {
          case TOPIC_FAST_MESSAGES:
            // the send time is encoded inside the message
            JsonNode msg = mapper.readTree(record.value());
            switch (msg.get("type").asText()) {
              case "test":
                // create a Document and set an _id, in this case the message number (document will be updated each time)
                Document messageDocument = MapRDB.newDocument(msg);
                messageDocument.setId( Integer.toString(messageDocument.getInt("k")));
                fastMessagesTable.insertOrReplace( messageDocument );

                long latency = (long) ((System.nanoTime() * 1e-9 - msg.get("t").asDouble()) * 1000);
                stats.recordValue(latency);
                global.recordValue(latency);
                break;
              case "marker":
                // whenever we get a marker message, we should dump out the stats
                // note that the number of fast messages won't necessarily be quite constant
                System.out.printf("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                        stats.getTotalCount(),
                        stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
                        stats.getMean(), stats.getValueAtPercentile(99));
                System.out.printf("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)\n",
                        global.getTotalCount(),
                        global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                        global.getMean(), global.getValueAtPercentile(99));
                stats.reset();
                break;
              default:
                throw new IllegalArgumentException("Illegal message type: " + msg.get("type"));
            }
            break;
          case TOPIC_SUMMARY_MARKERS:
            break;
          default:
            throw new IllegalStateException("Shouldn't be possible to get message on topic " + record.topic());
        }
      }
    }
  }

  private static Table getTable(String tablePath) {

    if ( ! MapRDB.tableExists(tablePath)) {
      return MapRDB.createTable(tablePath);
    } else {
      return MapRDB.getTable(tablePath);
    }

  }

}
