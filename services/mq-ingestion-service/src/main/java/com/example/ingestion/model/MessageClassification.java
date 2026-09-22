package com.example.ingestion.model;

public record MessageClassification(
        String format,
        String messageType,
        String networkPriority,
        String businessPriority,
        String route) {
}
