package com.rail.platform.graph.domain.model;

/**
 * The domain input to Engine 1a: a primary delay observed for a trip at a
 * station. Decoupled from the wire format — the Kafka adapter maps an inbound
 * {@code PositionEvent} into this value object so the propagation use case has no
 * dependency on Protobuf.
 *
 * @param tripId        affected journey (the cascade partition key)
 * @param stationIndex  internal topology index where the delay was observed
 * @param delaySeconds  signed schedule deviation; only positive delays cascade
 * @param originEventId idempotency id of the triggering event
 * @param eventTimeMs   event time (epoch millis)
 */
public record DelaySource(String tripId, int stationIndex, int delaySeconds,
                          String originEventId, long eventTimeMs) {
}
