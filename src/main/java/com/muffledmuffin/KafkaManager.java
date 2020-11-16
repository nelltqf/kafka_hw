package com.muffledmuffin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaManager.class);

    private static final String TOPIC_NAME = "sample_topic";

    private static final String APPLICATION_ID = "hello_world_v1";

    private final Properties globalProperties = new Properties();

    private final KafkaProducer<String, String> producer;

    public KafkaManager() {
        // TODO get from prop file
        globalProperties.put("bootstrap.servers", "localhost:9092");
        globalProperties.put("topic", TOPIC_NAME);
        globalProperties.put("admin_client", "true");

        producer = buildProducer();
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
}
