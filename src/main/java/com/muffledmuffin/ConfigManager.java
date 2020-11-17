package com.muffledmuffin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.Collections;
import java.util.Properties;

@Configuration
@PropertySource("classpath:kafka.properties")
public class ConfigManager {

    @Value("${topic_name}")
    private String topicName;

    @Value("${bootstrap.servers}")
    private String servers;

    @Value("${application_id}")
    private String applicationId;

    @Value("${admin_client}")
    private String adminClient;

    @Value("${CONNECTIONS_MAX_IDLE_MS_CONFIG}")
    private String connectionsMaxIdle;

    @Value("${COMPRESSION_TYPE_CONFIG}")
    private String compressionType;

    @Value("${MAX_POLL_RECORDS_CONFIG}")
    private String maxPollRecords;

    @Value("${AUTO_OFFSET_RESET_CONFIG}")
    private String autoOffsetReset;

    @Bean
    public Properties globalProperties() {
        Properties globalProperties = new Properties();

        globalProperties.put("bootstrap.servers", servers);
        globalProperties.put("topic", topicName);
        globalProperties.put("admin_client", adminClient);

        return globalProperties;
    }

    @Bean
    public KafkaProducer<String, String> producer(Properties globalProperties) {
        Properties producerProperties = new Properties();
        producerProperties.putAll(globalProperties);
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId);
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        producerProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdle);
        producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer<>(producerProperties);
    }

    @Bean
    public KafkaConsumer<String, String> consumer(Properties globalProperties) {
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(globalProperties);
        // TODO make a constant
        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "some_consumer");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "some_group");

        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singleton(topicName));
        return consumer;
    }

    @Bean
    public AdminClient adminClient(Properties globalProperties) {
        return AdminClient.create(globalProperties);
    }

    @Bean
    public KafkaManager kafkaManager(KafkaProducer<String, String> producer,
                                     KafkaConsumer<String, String> consumer,
                                     AdminClient adminClient) {
        return new KafkaManager(producer, consumer, topicName, adminClient);
    }
}
