package com.example;

import java.util.Map;
import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

public class Main {
    public static void main(String[] args) {

        Properties prop = new Properties();

        prop.setProperty("bootstrap.servers", "192.168.56.102:29092,192.168.56.102:29093,192.168.56.102:29094");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // nastavenie potvrdenia zapísu + zvolit interval alebo impl vlastneho mechanizmu
        prop.setProperty("enable.auto.commit", "false");   
        prop.setProperty("group.id", "test1"); 
        //prop.setProperty("auto.commit.interval", "1000");

        // ensure that on each start of consumer, records will be consumed from beggining
        prop.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(prop);
        Duration pollDuration = Duration.ofSeconds(1);

// try to list topics as we can verify the communication
try {
    Map<String, List<PartitionInfo>> topicMap =  new HashMap<String, List<PartitionInfo>>();
    topicMap = consumer.listTopics(); 
    String newLine = System.getProperty("line.separator");//This will retrieve line separator dependent on OS.
    for (String topic : topicMap.keySet()) {
        System.out.println("Topic fainged from broker " + topic + " and its partitions: " + newLine + topicMap.get(topic) + newLine);
    }


    // consume records 
    consumer.subscribe(Arrays.asList("replicatedTopic"));
    System.out.println(" Going to poll the records");
    while (true) {
   
        ConsumerRecords<Integer, String> records = consumer.poll(pollDuration);
         for (ConsumerRecord<Integer,String> consumerRecord : records) {
            System.out.println( newLine + "record offest: " + consumerRecord.offset() + newLine + "record key: " + consumerRecord.key() + newLine+ consumerRecord.value() + newLine);
    
         }
     }

} catch (Exception e) {
    System.err.println("Error occured during bootstraping..");
    e.printStackTrace();

}






finally{
    consumer.close();
}
      
    }
}