package com.example.replay.model;

import java.time.Instant;
import java.util.UUID;

public record ReplayJob(
        UUID id,
        String region,
        int partition,
        long startOffset,
        long endOffsetExclusive,
        long nextOffset,
        int maxRecords,
        int recordsPerSecond,
        String reason,
        String incidentId,
        String requestedBy,
        String approvedBy,
        Instant approvedAt,
        String status,
        int replayedCount,
        String errorMessage,
        Instant createdAt,
        Instant updatedAt) {
}
