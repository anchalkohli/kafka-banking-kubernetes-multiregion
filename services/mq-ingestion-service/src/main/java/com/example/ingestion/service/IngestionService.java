package com.example.ingestion.service;

import com.example.ingestion.model.MessageClassification;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
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
    private final Counter published;
    private final Counter publishFailures;
    private final int maxMessageBytes;
    private final String region;

    public IngestionService(KafkaTemplate<String, String> kafkaTemplate,
                            MessageClassifier classifier,
                            TopicRouter topicRouter,
                            MeterRegistry registry,
                            @Value("${app.ingestion.max-message-bytes:5242880}") int maxMessageBytes,
                            @Value("${app.region}") String region) {
        this.kafkaTemplate = kafkaTemplate;
        this.classifier = classifier;
        this.topicRouter = topicRouter;
        this.published = registry.counter("banking.ingestion.published");
        this.publishFailures = registry.counter("banking.ingestion.publish.failures");
        this.maxMessageBytes = maxMessageBytes;
        this.region = region;
    }

    public void processAndSendToKafka(String payload, String sourceMessageId, String correlationId) {
        validateSize(payload);
        MessageClassification classification = classifier.classify(payload);
        String topic = topicRouter.topicFor(classification.route());
        String messageId = sourceMessageId == null || sourceMessageId.isBlank()
                ? UUID.randomUUID().toString() : sourceMessageId;
        String key = messageId;

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        addHeader(record, "messageId", messageId);
        addHeader(record, "correlationId", correlationId);
        addHeader(record, "source", "IBM_MQ");
        addHeader(record, "region", region);
        addHeader(record, "format", classification.format());
        addHeader(record, "messageType", classification.messageType());
        addHeader(record, "networkPriority", classification.networkPriority());
        addHeader(record, "businessPriority", classification.businessPriority());
        addHeader(record, "route", classification.route());

        try {
            kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            published.increment();
        } catch (Exception ex) {
            publishFailures.increment();
            throw new IllegalStateException("Kafka publish failed; JMS transaction will roll back", ex);
        }
    }

    private void validateSize(String payload) {
        if (payload == null) throw new IllegalArgumentException("MQ payload must not be null");
        int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > maxMessageBytes) {
            throw new IllegalArgumentException("MQ payload exceeds configured maximum of " + maxMessageBytes + " bytes");
        }
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null && !value.isBlank()) {
            record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
