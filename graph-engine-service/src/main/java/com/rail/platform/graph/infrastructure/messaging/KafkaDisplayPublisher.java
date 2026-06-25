package com.rail.platform.graph.infrastructure.messaging;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.protobuf.Message;
import com.rail.platform.graph.domain.port.out.DisplayPublisherPort;
import com.rail.platform.schemas.display.DisplayMessage;

import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

/**
 * Kafka adapter implementing {@link DisplayPublisherPort}: streams departure-board
 * {@link DisplayMessage}s to {@code rail.display}, keyed by line ("canal") via the
 * record key so the {@link TripIdPartitioner} keeps each canal's messages on one
 * partition. Reuses the protobuf/Schema-Registry sender that backs the cascade
 * stream.
 */
@Component
public class KafkaDisplayPublisher implements DisplayPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaDisplayPublisher.class);

    private final KafkaSender<String, Message> sender;
    private final String displayTopic;

    public KafkaDisplayPublisher(KafkaSender<String, Message> cascadeSender,
                                 @Value("${rail.kafka.topics.display}") String displayTopic) {
        this.sender = cascadeSender;
        this.displayTopic = displayTopic;
    }

    @Override
    public Mono<Void> publish(List<DisplayMessage> messages) {
        if (messages.isEmpty()) {
            return Mono.empty();
        }
        Flux<SenderRecord<String, Message, String>> records = Flux.fromIterable(messages)
                .map(m -> SenderRecord.create(
                        new ProducerRecord<>(displayTopic, m.getTripId(), (Message) m), m.getMessageId()));
        return sender.send(records)
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        log.warn("display publish failed for msg={}: {}",
                                result.correlationMetadata(), result.exception().getMessage());
                    }
                })
                .then();
    }
}
