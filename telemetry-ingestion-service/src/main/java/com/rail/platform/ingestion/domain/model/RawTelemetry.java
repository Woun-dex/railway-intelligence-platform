package com.rail.platform.ingestion.domain.model;

import java.util.Objects;

/**
 * Value Object: an inbound raw telemetry frame as received at the boundary,
 * before any interpretation.
 *
 * <p>It pairs the declared {@link TelemetryKind} with the original payload
 * bytes exactly as they arrived. Keeping the verbatim bytes is essential: if
 * normalization fails, these very bytes are what gets dead-lettered, so the
 * frame can later be replayed or inspected without loss.
 *
 * <p>The byte array is referenced, not copied, for throughput; callers must not
 * mutate a buffer after handing it to a {@code RawTelemetry}.
 */
public record RawTelemetry(TelemetryKind kind, byte[] body) {

    public RawTelemetry {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(body, "body");
    }

    public static RawTelemetry of(TelemetryKind kind, byte[] body) {
        return new RawTelemetry(kind, body);
    }
}
