package com.example.replay.controller;

import com.example.replay.service.KafkaReplayService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/replay")
public class ReplayController {
    private final KafkaReplayService replayService;
    public ReplayController(KafkaReplayService replayService) { this.replayService = replayService; }

    @PostMapping("/{region}")
    @PreAuthorize("hasRole('dlq-admin')")
    public ResponseEntity<String> replay(@PathVariable String region) {
        int count = replayService.replayRegion(region);
        return ResponseEntity.ok("Replayed " + count + " records for " + region.toUpperCase());
    }
}
