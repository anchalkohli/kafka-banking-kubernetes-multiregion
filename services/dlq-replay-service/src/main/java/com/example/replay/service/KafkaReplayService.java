package com.example.replay.service;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaReplayService {
    private static final Set<String> ALLOWED_REGIONS = Set.of("emea", "nam", "aspac");
    private final String bootstrapServers;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaReplayService(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        Map<String, Object> producerProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true
        );
        this.kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    public int replayRegion(String requestedRegion) {
        String region = requestedRegion.toLowerCase(Locale.ROOT);
        if (!ALLOWED_REGIONS.contains(region)) throw new IllegalArgumentException("Unsupported region: " + requestedRegion);
        String sourceTopic = "dlq-payments-" + region;
        String targetTopic = "raw-payments-" + region;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-dlq-replay-" + region);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(sourceTopic).stream().map(info -> new TopicPartition(sourceTopic, info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    kafkaTemplate.send(targetTopic, record.key(), record.value()).get();
                    replayed++;
                }
                if (!records.isEmpty()) consumer.commitSync();
                boolean complete = partitions.stream().allMatch(tp -> consumer.position(tp) >= endOffsets.get(tp));
                if (complete) break;
            }
        } catch (Exception ex) {
            throw new IllegalStateException("DLQ replay failed for region " + region, ex);
        }
        return replayed;
    }
}
