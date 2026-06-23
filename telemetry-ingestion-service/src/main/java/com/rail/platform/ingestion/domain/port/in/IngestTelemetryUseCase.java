package com.rail.platform.ingestion.domain.port.in;

import com.rail.platform.ingestion.domain.model.RawTelemetry;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Inbound (driving) port of the ingestion bounded context.
 *
 * <p>This is the single entry point the outside world drives the context
 * through. Driving adapters — the REST gateway today, a Kafka source or a gRPC
 * endpoint tomorrow — depend only on this interface, never on the concrete
 * application service. The contract is intentionally tiny: hand the context a
 * raw frame (or a stream of them) and it takes responsibility for normalizing,
 * publishing, or dead-lettering it.
 *
 * <p>Reactor types appear here deliberately: this is a reactive context and the
 * port models the asynchronous completion of ingestion. The domain
 * <em>model</em> (value objects) remains free of any framework dependency.
 */
public interface IngestTelemetryUseCase {

    /** Ingest a single raw frame. Never propagates a parse error to the caller. */
    Mono<Void> ingest(RawTelemetry raw);

    /** Ingest a stream of raw frames with bounded in-flight concurrency. */
    Mono<Void> ingestStream(Flux<RawTelemetry> stream);
}
