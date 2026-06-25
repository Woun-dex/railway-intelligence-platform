package com.rail.platform.graph.application;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.rail.platform.schemas.display.CriticalityEvent;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * In-memory hot store for the most recent Criticality Score per station, plus a
 * multicast stream the viewer subscribes to over SSE. Keeps the latest score per
 * station (last-write-wins) so {@code GET /graph/criticality/top} can return a
 * live leaderboard without replaying Kafka.
 */
@Component
public class CriticalityState {

    private final Map<Integer, CriticalityEvent> latest = new ConcurrentHashMap<>();
    private final Sinks.Many<CriticalityEvent> sink =
            Sinks.many().multicast().onBackpressureBuffer(256, false);

    public void record(CriticalityEvent event) {
        latest.put(event.getStationId(), event);
        sink.tryEmitNext(event);
    }

    /** Highest-scoring stations right now, descending by total CS. */
    public List<CriticalityEvent> top(int k) {
        return latest.values().stream()
                .sorted(Comparator.comparingDouble((CriticalityEvent e) -> e.getScore().getTotal()).reversed())
                .limit(Math.max(1, k))
                .toList();
    }

    public Flux<CriticalityEvent> stream() {
        return sink.asFlux();
    }

    public int size() {
        return latest.size();
    }
}
