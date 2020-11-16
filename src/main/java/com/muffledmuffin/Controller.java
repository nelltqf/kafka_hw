package com.muffledmuffin;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    private final KafkaManager kafkaManager;

    public Controller(KafkaManager kafkaManager) {
        this.kafkaManager = kafkaManager;
    }

    @GetMapping
    public String getStub() {
        return "Stub";
    }
}
