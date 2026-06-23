package com.rail.platform.ingestion.domain.model;

import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;

/**
 * Value Object: the identity of a single journey.
 *
 * <p>{@code TripId} is the cornerstone invariant of the ingestion context. It
 * is the Kafka partition key, and partitioning by it is what guarantees
 * <em>causal ordering per journey</em> across parallel consumer workers (see
 * the architecture's "ordered processing per train" rule). The invariant is
 * simple but non-negotiable: a journey identity must be present and non-blank —
 * a telemetry signal that cannot be attributed to a trip cannot preserve order
 * and is therefore not admissible to the bus.
 *
 * <p>Being a record, equality is by value, so a {@code TripId} can be used
 * safely as a map key or compared across events.
 */
public record TripId(String value) {

    public TripId {
        if (value == null || value.isBlank()) {
            throw new PayloadNormalizationException("trip_id (partition key) is mandatory");
        }
        value = value.trim();
    }

    public static TripId of(String value) {
        return new TripId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
