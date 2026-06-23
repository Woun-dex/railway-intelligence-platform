package com.rail.platform.ingestion.domain.model;

/**
 * The kind of raw telemetry entering the ingestion bounded context.
 *
 * <p>This is a domain concept — the three structural categories of upstream
 * signal the platform reasons about — deliberately free of any transport
 * (Kafka topic) or wire-format (Protobuf type) detail. The mapping from a kind
 * to its destination topic and published-language message lives in the
 * infrastructure adapters.
 */
public enum TelemetryKind {
    POSITION,
    SIGNALLING,
    INCIDENT
}
