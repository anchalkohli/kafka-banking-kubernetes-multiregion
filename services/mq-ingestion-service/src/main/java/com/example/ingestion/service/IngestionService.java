package com.example.ingestion.service;

import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class IngestionService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String rawTopic;

    public IngestionService(KafkaTemplate<String, String> kafkaTemplate,
                            @Value("${app.kafka.topic}") String rawTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.rawTopic = rawTopic;
    }

    public void processAndSendToKafka(String payload) {
        try {
            kafkaTemplate.send(rawTopic, payload).get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Kafka publish failed; JMS transaction will roll back", ex);
        }
    }
}
