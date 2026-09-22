package com.example.ingestion.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TopicRouter {
    private final String critical;
    private final String standard;
    private final String reporting;
    private final String quarantine;

    public TopicRouter(
            @Value("${app.kafka.topics.critical}") String critical,
            @Value("${app.kafka.topics.standard}") String standard,
            @Value("${app.kafka.topics.reporting}") String reporting,
            @Value("${app.kafka.topics.quarantine}") String quarantine) {
        this.critical = critical;
        this.standard = standard;
        this.reporting = reporting;
        this.quarantine = quarantine;
    }

    public String topicFor(String route) {
        return switch (route) {
            case "critical" -> critical;
            case "standard" -> standard;
            case "reporting" -> reporting;
            default -> quarantine;
        };
    }
}
