package com.example.replay.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ReplayCommand(
        @NotNull @Min(0) Integer partition,
        @NotNull @Min(0) Long startOffset,
        @NotNull @Min(1) Long endOffsetExclusive,
        @NotNull @Min(1) @Max(10000) Integer maxRecords,
        @NotNull @Min(1) @Max(2000) Integer recordsPerSecond,
        @NotBlank String reason,
        String incidentId) {
}
