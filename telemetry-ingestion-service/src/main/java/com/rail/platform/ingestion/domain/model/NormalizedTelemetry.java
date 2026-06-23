package com.rail.platform.ingestion.domain.model;

import com.google.protobuf.Message;

import java.util.Objects;

/**
 * Value Object: the result of successfully normalizing a raw frame into the
 * platform's published language.
 *
 * <p>It carries three things the rest of the platform needs:
 * <ul>
 *   <li>the {@link TelemetryKind} (so a transport adapter can resolve the
 *       destination topic),</li>
 *   <li>the {@link TripId} partition key (so causal ordering is preserved),</li>
 *   <li>the {@link Message} — a Protobuf contract from {@code shared-schemas},
 *       the platform's shared-kernel published language.</li>
 * </ul>
 *
 * <p>This is the hand-off contract between the application service and the
 * outbound {@code TelemetryPublisherPort}; it knows nothing about Kafka.
 */
public record NormalizedTelemetry(TelemetryKind kind, TripId key, Message payload) {

    public NormalizedTelemetry {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(payload, "payload");
    }
}
