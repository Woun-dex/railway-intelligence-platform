package com.rail.platform.ingestion.infrastructure.messaging;

import java.util.EnumMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.protobuf.Message;
import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;
import com.rail.platform.ingestion.domain.model.TelemetryKind;
import com.rail.platform.ingestion.domain.port.out.TelemetryPublisherPort;

import jakarta.annotation.PostConstruct;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Kafka adapter implementing {@link TelemetryPublisherPort}.
 *
 * <p>It resolves the destination topic from the telemetry {@link TelemetryKind},
 * uses the {@link com.rail.platform.ingestion.domain.model.TripId} as the
 * Kafka key (so {@link TripIdPartitioner} preserves per-journey ordering), and
 * serializes the published-language payload via the protobuf sender.
 */
@Component
public class KafkaTelemetryPublisher implements TelemetryPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaTelemetryPublisher.class);

    private final KafkaSender<String, Message> sender;
    private final Map<TelemetryKind, String> topicByKind = new EnumMap<>(TelemetryKind.class);

    @Value("${rail.kafka.topics.position}")
    private String positionTopic;
    @Value("${rail.kafka.topics.signalling}")
    private String signallingTopic;
    @Value("${rail.kafka.topics.incident}")
    private String incidentTopic;

    public KafkaTelemetryPublisher(KafkaSender<String, Message> telemetrySender) {
        this.sender = telemetrySender;
    }

    @PostConstruct
    void bindTopics() {
        topicByKind.put(TelemetryKind.POSITION, positionTopic);
        topicByKind.put(TelemetryKind.SIGNALLING, signallingTopic);
        topicByKind.put(TelemetryKind.INCIDENT, incidentTopic);
    }

    @Override
    public Mono<Void> publish(NormalizedTelemetry telemetry) {
        return publishAll(Flux.just(telemetry));
    }

    @Override
    public Mono<Void> publishAll(Flux<NormalizedTelemetry> stream) {
        // One send() for the whole stream: reactor-kafka pipelines the records
        // and the underlying producer batches them (linger + batch.size). This
        // is dramatically faster than one send() per record.
        return sender.send(stream.map(this::toRecord))
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        log.warn("publish failed for key={}: {}",
                                result.correlationMetadata(), result.exception().getMessage());
                    }
                })
                .then();
    }

    private SenderRecord<String, Message, String> toRecord(NormalizedTelemetry telemetry) {
        String topic = topicByKind.get(telemetry.kind());
        String key = telemetry.key().value();
        return SenderRecord.create(new ProducerRecord<>(topic, key, telemetry.payload()), key);
    }
}
