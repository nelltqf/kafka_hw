package com.muffledmuffin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaManager.class);

    private final KafkaProducer<String, String> producer;

    private final KafkaConsumer<String, String> consumer;

    private final String topicName;

    private final AdminClient adminClient;

    public KafkaManager(KafkaProducer<String, String> producer,
                        KafkaConsumer<String, String> consumer,
                        String topicName,
                        AdminClient adminClient) {
        this.producer = producer;
        this.consumer = consumer;
        this.topicName = topicName;
        this.adminClient = adminClient;
    }

    public boolean doesTopicExist() {
        ListTopicsResult topicsList = adminClient.listTopics();
        try {
            Set<String> resultNames = topicsList.names().get();
            List<String> topicNames = resultNames
                    .stream()
                    .filter(name -> !name.startsWith("_"))
                    .collect(Collectors.toList());
            return topicNames.contains(topicName);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    public ClusterInfo clusterInfo() {
        try {
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            // TODO futures in stream
            return ClusterInfo.builder()
                    .id(clusterResult.clusterId().get())
                    .nodes(clusterResult.nodes().get().toString())
                    .authorizedOperations(clusterResult.authorizedOperations().get())
                    .build();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void sendMessages(List<String> messages) {
        for (String message : messages) {
            try {
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topicName, message)).get();
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
                RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topicName, message)).get();
                //TODO add logback conf to dump everything in a log file
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
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Received [{}] at partition {}, offset {}", record.value(), record.partition(),
                        record.offset());
                result.add(record.value());
            }
        }
        LOGGER.info("Received {} messages", result.size());
        return result;
    }
}
