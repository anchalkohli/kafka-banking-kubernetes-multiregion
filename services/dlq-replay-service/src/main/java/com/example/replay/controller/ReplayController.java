package com.example.replay.controller;

import com.example.replay.model.ReplayCommand;
import com.example.replay.model.ReplayJob;
import com.example.replay.service.KafkaReplayService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/replay")
public class ReplayController {
    private final KafkaReplayService replayService;

    public ReplayController(KafkaReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/{region}")
    @PreAuthorize("hasRole('dlq-admin')")
    public ResponseEntity<ReplayJob> replay(@PathVariable String region,
                                            @Valid @RequestBody ReplayCommand command,
                                            Authentication authentication) {
        return ResponseEntity.ok(replayService.createAndRun(region, command, authentication.getName()));
    }

    @PostMapping("/jobs/{jobId}/resume")
    @PreAuthorize("hasRole('dlq-admin')")
    public ResponseEntity<ReplayJob> resume(@PathVariable UUID jobId, Authentication authentication) {
        return ResponseEntity.ok(replayService.resume(jobId, authentication.getName()));
    }

    @GetMapping("/jobs/{jobId}")
    @PreAuthorize("hasRole('dlq-admin')")
    public ResponseEntity<ReplayJob> status(@PathVariable UUID jobId) {
        return ResponseEntity.ok(replayService.get(jobId));
    }
}
