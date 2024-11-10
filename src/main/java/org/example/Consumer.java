package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // Use a unique group id
        properties.put("group.id", "test-group-" + System.currentTimeMillis());
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("auto.offset.reset", "earliest");
        properties.put("enable.auto.commit", "true");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        System.out.println("Starting consumer...");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList("test-topic"));
            System.out.println("Subscribed to test-topic");

            while (true) {
                System.out.println("Polling...");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                System.out.printf("Got %d records%n", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received: %s%n", record.value());
                }
            }
        }
    }
}