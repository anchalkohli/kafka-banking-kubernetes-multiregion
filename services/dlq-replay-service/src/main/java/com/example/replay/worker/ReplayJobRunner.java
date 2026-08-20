package com.example.replay.worker;

import com.example.replay.service.KafkaReplayService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@ConditionalOnProperty(name = "replay.executor.enabled", havingValue = "true")
public class ReplayJobRunner implements CommandLineRunner {
    private final KafkaReplayService replayService;
    private final ConfigurableApplicationContext context;

    public ReplayJobRunner(KafkaReplayService replayService, ConfigurableApplicationContext context) {
        this.replayService = replayService;
        this.context = context;
    }

    @Override
    public void run(String... args) {
        String rawJobId = context.getEnvironment().getProperty("replay.job-id");
        if (rawJobId == null || rawJobId.isBlank()) {
            throw new IllegalStateException("REPLAY_JOB_ID is required when replay executor mode is enabled");
        }

        replayService.executeApproved(UUID.fromString(rawJobId));
        int exitCode = SpringApplication.exit(context, () -> 0);
        System.exit(exitCode);
    }
}
