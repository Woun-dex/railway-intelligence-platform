package com.rail.platform.ingestion.domain.model;

import java.util.Objects;

/**
 * Value Object: a raw frame that could not be admitted, together with the
 * reason it was rejected.
 *
 * <p>Dead-lettering is a first-class domain outcome here, not an error swallowed
 * in a catch block: the verbatim {@code rawBody} is preserved so the frame is
 * never lost, and the {@code reason} records why normalization failed for later
 * triage. The {@link TelemetryKind} is retained so consumers of the DLQ can
 * route by category.
 */
public record DeadLetter(TelemetryKind kind, byte[] rawBody, String reason) {

    public DeadLetter {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(rawBody, "rawBody");
    }

    /** Build a dead letter from the original frame and a failure reason. */
    public static DeadLetter from(RawTelemetry raw, String reason) {
        return new DeadLetter(raw.kind(), raw.body(), reason == null ? "unknown" : reason);
    }
}
