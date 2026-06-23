package com.rail.platform.graph.infrastructure.messaging;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.protobuf.Message;
import com.rail.platform.graph.domain.port.out.CascadePublisherPort;
import com.rail.platform.schemas.prediction.CascadeEvent;

import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Kafka adapter implementing {@link CascadePublisherPort}: streams a trip's
 * cascade events to {@code rail.graph.cascade}, keyed by {@code trip_id} so the
 * {@link TripIdPartitioner} keeps per-journey order.
 */
@Component
public class KafkaCascadePublisher implements CascadePublisherPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaCascadePublisher.class);

    private final KafkaSender<String, Message> sender;
    private final String cascadeTopic;

    public KafkaCascadePublisher(KafkaSender<String, Message> cascadeSender,
                                 @Value("${rail.kafka.topics.cascade}") String cascadeTopic) {
        this.sender = cascadeSender;
        this.cascadeTopic = cascadeTopic;
    }

    @Override
    public Mono<Void> publish(String tripId, List<CascadeEvent> events) {
        if (events.isEmpty()) {
            return Mono.empty();
        }
        Flux<SenderRecord<String, Message, String>> records = Flux.fromIterable(events)
                .map(e -> SenderRecord.create(
                        new ProducerRecord<>(cascadeTopic, tripId, (Message) e), tripId));
        return sender.send(records)
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        log.warn("cascade publish failed for trip={}: {}",
                                result.correlationMetadata(), result.exception().getMessage());
                    }
                })
                .then();
    }
}
