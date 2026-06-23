package com.rail.platform.ingestion.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.ingestion.domain.model.DeadLetter;
import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;
import com.rail.platform.ingestion.domain.model.RawTelemetry;
import com.rail.platform.ingestion.domain.port.in.IngestTelemetryUseCase;
import com.rail.platform.ingestion.domain.port.out.DeadLetterPort;
import com.rail.platform.ingestion.domain.port.out.TelemetryNormalizer;
import com.rail.platform.ingestion.domain.port.out.TelemetryPublisherPort;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Application service that realizes the {@link IngestTelemetryUseCase}.
 *
 * <p>It is the orchestrator at the centre of the hexagon: it wires the three
 * outbound ports — normalize (ACL), publish, dead-letter — into the one
 * business rule of the context:
 *
 * <pre>
 *   normalize(raw)  ──success──▶  publish(normalized)
 *                   ──failure──▶  deadLetter(raw, reason)
 * </pre>
 *
 * <p>It holds no transport or wire-format knowledge; every collaborator is a
 * domain port. Normalization failures are caught and quarantined so the caller
 * never sees an exception — the "zero message handling exceptions" DoD clause.
 * Publication failures are <em>not</em> dead-lettered: those are transient
 * infrastructure faults that the producer retries (idempotent, acks=all).
 */
@Service
public class TelemetryIngestionService implements IngestTelemetryUseCase {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);

    /** Bounded in-flight concurrency for the streaming path — caps memory. */
    private static final int STREAM_CONCURRENCY = 256;

    private final TelemetryNormalizer normalizer;
    private final TelemetryPublisherPort publisher;
    private final DeadLetterPort deadLetterPort;

    public TelemetryIngestionService(TelemetryNormalizer normalizer,
                                     TelemetryPublisherPort publisher,
                                     DeadLetterPort deadLetterPort) {
        this.normalizer = normalizer;
        this.publisher = publisher;
        this.deadLetterPort = deadLetterPort;
    }

    @Override
    public Mono<Void> ingest(RawTelemetry raw) {
        final NormalizedTelemetry normalized;
        try {
            normalized = normalizer.normalize(raw);
        } catch (PayloadNormalizationException e) {
            return quarantine(raw, e.getMessage());
        } catch (RuntimeException e) {
            return quarantine(raw, "normalization error: " + e.getMessage());
        }
        return publisher.publish(normalized);
    }

    @Override
    public Mono<Void> ingestStream(Flux<RawTelemetry> stream) {
        // Normalize (or quarantine) each frame concurrently, then hand the
        // surviving events to the publisher as ONE stream — letting the Kafka
        // adapter pipeline a single send() rather than one per record.
        Flux<NormalizedTelemetry> normalized =
                stream.flatMap(this::normalizeOrQuarantine, STREAM_CONCURRENCY);
        return publisher.publishAll(normalized);
    }

    /** Normalize a frame; on failure dead-letter it and drop it from the stream. */
    private Mono<NormalizedTelemetry> normalizeOrQuarantine(RawTelemetry raw) {
        try {
            return Mono.just(normalizer.normalize(raw));
        } catch (PayloadNormalizationException e) {
            return quarantine(raw, e.getMessage()).then(Mono.empty());
        } catch (RuntimeException e) {
            return quarantine(raw, "normalization error: " + e.getMessage()).then(Mono.empty());
        }
    }

    private Mono<Void> quarantine(RawTelemetry raw, String reason) {
        log.debug("DLQ [{}]: {}", raw.kind(), reason);
        return deadLetterPort.send(DeadLetter.from(raw, reason));
    }
}
