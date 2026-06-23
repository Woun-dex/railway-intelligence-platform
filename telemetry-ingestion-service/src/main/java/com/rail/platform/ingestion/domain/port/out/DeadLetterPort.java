package com.rail.platform.ingestion.domain.port.out;

import com.rail.platform.ingestion.domain.model.DeadLetter;

import reactor.core.publisher.Mono;

/**
 * Outbound port: routes an unprocessable frame to the dead-letter queue.
 *
 * <p>Keeping this separate from {@link TelemetryPublisherPort} is intentional:
 * admitting a valid event and quarantining a malformed one are two different
 * domain outcomes with different delivery guarantees, and an adapter may back
 * them with different topics, serializers, or even different brokers.
 */
public interface DeadLetterPort {

    Mono<Void> send(DeadLetter deadLetter);
}
