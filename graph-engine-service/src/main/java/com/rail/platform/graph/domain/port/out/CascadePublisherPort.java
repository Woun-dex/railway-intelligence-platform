package com.rail.platform.graph.domain.port.out;

import java.util.List;

import com.rail.platform.schemas.prediction.CascadeEvent;

import reactor.core.publisher.Mono;

/**
 * Outbound port for emitting cascade events. The published language is the shared
 * {@link CascadeEvent} contract; events for one trip are keyed by its
 * {@code tripId} so the per-journey order is preserved on
 * {@code rail.graph.cascade}.
 */
public interface CascadePublisherPort {

    Mono<Void> publish(String tripId, List<CascadeEvent> events);
}
