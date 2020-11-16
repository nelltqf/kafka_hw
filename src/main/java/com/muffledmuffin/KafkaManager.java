package com.muffledmuffin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaManager.class);

    private static final String TOPIC_NAME = "sample_topic";

    private static final String APPLICATION_ID = "hello_world_v1";

    private final Properties globalProperties = new Properties();

    private final KafkaProducer<String, String> producer;

    private final KafkaConsumer<String, String> consumer;

    public KafkaManager() {
        // TODO get from prop file
        globalProperties.put("bootstrap.servers", "localhost:9092");
        globalProperties.put("topic", TOPIC_NAME);
        globalProperties.put("admin_client", "true");

        producer = buildProducer();
        consumer = buildConsumer();
    }

    private KafkaProducer<String, String> buildProducer() {
        Properties producerProperties = new Properties();
        producerProperties.putAll(globalProperties);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, APPLICATION_ID);
        // TODO get from... somewhere
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer<>(producerProperties);
    }

    private KafkaConsumer<String, String> buildConsumer() {
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(globalProperties);
        // TODO well...
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "some_consumer");
        // TODO get from... somewhere
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "some_group");
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        return consumer;
    }

    public void sendMessages(List<String> messages) {
        for (String message: messages) {
            try {
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC_NAME, message)).get();
                LOGGER.info("Delivered [{}] at partition {}, offset {}", message, recordMetadata.partition(),
                        recordMetadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public void sendMessages(int numberOfMessages) {
        for (int i = 0; i < 10; i++) {
            try {
                String message = String.format("{\"userName\": \"user%d\"}", i);
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(TOPIC_NAME, message)).get();
                LOGGER.info("Delivered [{}] at partition {}, offset {}", message, recordMetadata.partition(),
                        recordMetadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public List<String> getMessages() {
        List<String> result = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, String> record: records) {
                LOGGER.info("Received [{}] at partition {}, offset {}", record.value(), record.partition(),
                        record.offset());
                result.add(record.value());
            }
        }
        LOGGER.info("Received {} messages", result.size());
        return result;
    }
}
