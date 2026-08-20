package com.example.replay.service;

import com.example.replay.model.ReplayCommand;
import com.example.replay.model.ReplayJob;
import com.example.replay.repository.ReplayJobRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaReplayService {
    private static final Set<String> ALLOWED_REGIONS = Set.of("emea", "nam", "aspac");
    private static final int CHECKPOINT_EVERY = 100;

    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ReplayJobRepository repository;
    private final DataSource dataSource;

    public KafkaReplayService(KafkaProperties kafkaProperties, ReplayJobRepository repository, DataSource dataSource) {
        this.kafkaProperties = kafkaProperties;
        this.repository = repository;
        this.dataSource = dataSource;

        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        this.kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    public ReplayJob createRequest(String requestedRegion, ReplayCommand command, String requestedBy) {
        String region = normalizeRegion(requestedRegion);
        validateCommand(command);
        return repository.create(region, command, requestedBy);
    }

    public ReplayJob approve(UUID jobId, String approvedBy) {
        ReplayJob job = get(jobId);
        if (!"PENDING_APPROVAL".equals(job.status())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Replay job is not awaiting approval");
        }
        if (job.requestedBy().equals(approvedBy)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Maker-checker violation: requester cannot approve the same replay job");
        }
        if (!repository.approve(jobId, approvedBy)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Replay job approval state changed concurrently");
        }
        return get(jobId);
    }

    public ReplayJob get(UUID jobId) {
        return repository.find(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Replay job not found"));
    }

    /**
     * Executes an already approved replay. This method is intended for the finite Kubernetes Job worker,
     * not for the long-running API Deployment.
     */
    public ReplayJob executeApproved(UUID jobId) {
        ReplayJob job = get(jobId);
        if (!"APPROVED".equals(job.status())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Replay job must be APPROVED before execution");
        }

        String sourceTopic = "dlq-payments-" + job.region();
        String targetTopic = "raw-payments-" + job.region();
        long nextOffset = job.nextOffset();
        int replayed = job.replayedCount();

        try (Connection lockConnection = dataSource.getConnection()) {
            if (!tryRegionLock(lockConnection, job.region())) {
                throw new ResponseStatusException(HttpStatus.CONFLICT,
                        "Another replay is already active for region " + job.region().toUpperCase(Locale.ROOT));
            }

            try {
                if (!repository.markRunningIfApproved(jobId)) {
                    throw new ResponseStatusException(HttpStatus.CONFLICT, "Replay job is no longer approved for execution");
                }

                Properties props = new Properties();
                props.putAll(kafkaProperties.buildConsumerProperties());
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "bounded-dlq-replay-" + job.region());
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

                TopicPartition tp = new TopicPartition(sourceTopic, job.partition());
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                    List<TopicPartition> partitions = consumer.partitionsFor(sourceTopic).stream()
                            .map(info -> new TopicPartition(sourceTopic, info.partition()))
                            .toList();
                    if (!partitions.contains(tp)) {
                        throw new IllegalArgumentException("Partition " + job.partition() + " does not exist on " + sourceTopic);
                    }

                    consumer.assign(List.of(tp));
                    long beginning = consumer.beginningOffsets(List.of(tp)).get(tp);
                    long end = consumer.endOffsets(List.of(tp)).get(tp);
                    long boundedEnd = Math.min(job.endOffsetExclusive(), end);

                    if (nextOffset < beginning || nextOffset > boundedEnd) {
                        throw new IllegalArgumentException("Replay offset is outside the retained Kafka range");
                    }

                    consumer.seek(tp, nextOffset);
                    long nanosPerRecord = TimeUnit.SECONDS.toNanos(1) / job.recordsPerSecond();
                    long nextAllowedSend = System.nanoTime();

                    boolean finished = false;
                    while (!finished && nextOffset < boundedEnd && replayed < job.maxRecords()) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                        if (records.isEmpty()) {
                            if (consumer.position(tp) >= boundedEnd) break;
                            continue;
                        }

                        for (ConsumerRecord<String, String> record : records.records(tp)) {
                            if (record.offset() < nextOffset) continue;
                            if (record.offset() >= boundedEnd || replayed >= job.maxRecords()) {
                                finished = true;
                                break;
                            }

                            throttle(nextAllowedSend);
                            kafkaTemplate.send(targetTopic, record.key(), record.value()).get(30, TimeUnit.SECONDS);
                            replayed++;
                            nextOffset = record.offset() + 1;
                            nextAllowedSend = System.nanoTime() + nanosPerRecord;

                            if (replayed % CHECKPOINT_EVERY == 0) {
                                repository.checkpoint(jobId, nextOffset, replayed);
                            }
                        }
                    }

                    repository.complete(jobId, nextOffset, replayed);
                }
            } catch (ResponseStatusException ex) {
                repository.fail(jobId, nextOffset, replayed, ex.getReason());
                throw ex;
            } catch (Exception ex) {
                repository.fail(jobId, nextOffset, replayed, ex.getMessage());
                throw new IllegalStateException("DLQ replay failed for region " + job.region(), ex);
            } finally {
                unlockRegion(lockConnection, job.region());
            }
        } catch (ResponseStatusException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to coordinate DLQ replay", ex);
        }
        return get(jobId);
    }

    private String normalizeRegion(String requestedRegion) {
        String region = requestedRegion.toLowerCase(Locale.ROOT);
        if (!ALLOWED_REGIONS.contains(region)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported region: " + requestedRegion);
        }
        return region;
    }

    private void validateCommand(ReplayCommand command) {
        if (command.startOffset() >= command.endOffsetExclusive()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "startOffset must be lower than endOffsetExclusive");
        }
        long requestedRange = command.endOffsetExclusive() - command.startOffset();
        if (requestedRange > command.maxRecords()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Requested offset range exceeds maxRecords; submit a smaller bounded replay request");
        }
    }

    private boolean tryRegionLock(Connection connection, String region) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement("select pg_try_advisory_lock(hashtext(?))")) {
            ps.setString(1, "dlq-replay:" + region);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private void unlockRegion(Connection connection, String region) {
        try (PreparedStatement ps = connection.prepareStatement("select pg_advisory_unlock(hashtext(?))")) {
            ps.setString(1, "dlq-replay:" + region);
            ps.executeQuery();
        } catch (Exception ignored) {
        }
    }

    private void throttle(long nextAllowedSend) throws InterruptedException {
        long sleepNanos = nextAllowedSend - System.nanoTime();
        if (sleepNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(sleepNanos);
        }
    }
}
