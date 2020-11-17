package com.muffledmuffin;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class Controller {

    private final KafkaManager kafkaManager;

    public Controller(KafkaManager kafkaManager) {
        this.kafkaManager = kafkaManager;
    }

    @PostMapping()
    public void sendMessage(@RequestBody List<String> messages) {
        kafkaManager.sendMessages(messages);
    }

    @GetMapping
    public List<String> getMessages() {
        return kafkaManager.getMessages();
    }

    @GetMapping("/checkTopic")
    public boolean checkTopic() {
        return kafkaManager.doesTopicExist();
    }

    @GetMapping("/clusterInfo")
    public ClusterInfo clusterInfo() {
        return kafkaManager.clusterInfo();
    }
}
