package com.rail.platform.ingestion.infrastructure.messaging;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.rail.platform.ingestion.domain.model.DeadLetter;
import com.rail.platform.ingestion.domain.port.out.DeadLetterPort;

import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Kafka adapter implementing {@link DeadLetterPort}.
 *
 * <p>Publishes the verbatim original bytes to the DLQ topic with diagnostic
 * headers (event kind, failure reason, ingest timestamp) so a malformed payload
 * is never lost and can be triaged or replayed.
 */
@Component
public class KafkaDeadLetterAdapter implements DeadLetterPort {

    private final KafkaSender<String, byte[]> dlqSender;

    @Value("${rail.kafka.topics.dlq}")
    private String dlqTopic;

    public KafkaDeadLetterAdapter(KafkaSender<String, byte[]> dlqSender) {
        this.dlqSender = dlqSender;
    }

    @Override
    public Mono<Void> send(DeadLetter deadLetter) {
        ProducerRecord<String, byte[]> dlqRecord =
                new ProducerRecord<>(dlqTopic, UUID.randomUUID().toString(), deadLetter.rawBody());
        dlqRecord.headers()
                .add(new RecordHeader("x-event-type",
                        deadLetter.kind().name().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-error-reason",
                        safe(deadLetter.reason()).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("x-ingest-ts",
                        Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

        SenderRecord<String, byte[], String> record = SenderRecord.create(dlqRecord, dlqRecord.key());
        return dlqSender.send(Mono.just(record)).then();
    }

    private static String safe(String s) {
        return s == null ? "unknown" : s;
    }
}
