package com.rail.platform.ingestion.domain.port.out;

import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Outbound port: publishes admitted telemetry to the platform backbone.
 *
 * <p>The application service depends on this abstraction, not on Kafka. The
 * concrete adapter resolves the destination topic from the telemetry's
 * {@link NormalizedTelemetry#kind() kind}, uses its
 * {@link NormalizedTelemetry#key() key} as the partition key (preserving
 * per-journey causal ordering), and serializes the published-language payload.
 */
public interface TelemetryPublisherPort {

    /** Publish a single event. */
    Mono<Void> publish(NormalizedTelemetry telemetry);

    /**
     * Publish a stream of events as one pipelined operation. Adapters backed by
     * a streaming producer (reactor-kafka) are far more efficient fed a whole
     * {@link Flux} than called once per record, so the high-throughput batch
     * path routes here. Completes when every record has been acknowledged.
     */
    Mono<Void> publishAll(Flux<NormalizedTelemetry> stream);
}
