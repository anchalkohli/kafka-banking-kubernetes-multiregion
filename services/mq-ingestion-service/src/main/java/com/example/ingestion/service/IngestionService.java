package com.example.ingestion.service;

import com.example.ingestion.model.MessageClassification;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class IngestionService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MessageClassifier classifier;
    private final TopicRouter topicRouter;

    public IngestionService(KafkaTemplate<String, String> kafkaTemplate,
                            MessageClassifier classifier,
                            TopicRouter topicRouter) {
        this.kafkaTemplate = kafkaTemplate;
        this.classifier = classifier;
        this.topicRouter = topicRouter;
    }

    public void processAndSendToKafka(String payload) {
        MessageClassification classification = classifier.classify(payload);
        String topic = topicRouter.topicFor(classification.route());

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);
        addHeader(record, "messageId", UUID.randomUUID().toString());
        addHeader(record, "format", classification.format());
        addHeader(record, "messageType", classification.messageType());
        addHeader(record, "networkPriority", classification.networkPriority());
        addHeader(record, "businessPriority", classification.businessPriority());
        addHeader(record, "route", classification.route());

        try {
            kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Kafka publish failed; JMS transaction will roll back", ex);
        }
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }
}
