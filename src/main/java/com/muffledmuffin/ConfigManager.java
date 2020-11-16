package com.muffledmuffin;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigManager {

    @Bean
    public KafkaManager kafkaManager() {
        return new KafkaManager();
    }
}
