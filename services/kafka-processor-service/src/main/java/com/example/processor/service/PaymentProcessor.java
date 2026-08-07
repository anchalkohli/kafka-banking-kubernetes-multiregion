package com.example.processor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class PaymentProcessor {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String outputTopic;
    private final String dlqTopic;
    private final String region;

    public PaymentProcessor(KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper,
                            @Value("${app.kafka.output-topic}") String outputTopic,
                            @Value("${app.kafka.dlq-topic}") String dlqTopic,
                            @Value("${app.region}") String region) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.outputTopic = outputTopic;
        this.dlqTopic = dlqTopic;
        this.region = region;
    }

    @KafkaListener(topics = "${app.kafka.input-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void process(ConsumerRecord<String, String> record) throws Exception {
        try {
            Map<String, Object> normalized = new LinkedHashMap<>();
            normalized.put("region", region);
            normalized.put("messageKey", record.key());
            normalized.put("sourceTopic", record.topic());
            normalized.put("sourcePartition", record.partition());
            normalized.put("sourceOffset", record.offset());
            normalized.put("processedAt", Instant.now().toString());
            normalized.put("rawMessage", record.value());
            kafkaTemplate.send(outputTopic, record.key(), objectMapper.writeValueAsString(normalized)).get();
        } catch (Exception ex) {
            kafkaTemplate.send(dlqTopic, record.key(), record.value()).get();
        }
    }
}
