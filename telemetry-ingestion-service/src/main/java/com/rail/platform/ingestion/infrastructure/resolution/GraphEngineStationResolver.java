package com.rail.platform.ingestion.infrastructure.resolution;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * {@link StationIndexPort} adapter that loads the {@code stop_id → index} map from
 * the graph engine's {@code /graph/stopmap} and caches it in heap, refreshing on a
 * fixed cadence so re-versioned topologies are picked up. If the engine is
 * unavailable the cache stays as-is (empty at boot), and resolution returns
 * {@code null} — telemetry still flows, just without a resolved station, exactly
 * as before this layer existed.
 *
 * <p>Resolution tolerates the common feed prefixes (e.g. {@code StopPoint:Q:43135},
 * {@code IDFM:463685}) by retrying on the substring after the last {@code ':'}.
 */
@Component
public class GraphEngineStationResolver
        implements com.rail.platform.ingestion.domain.port.out.StationIndexPort {

    private static final Logger log = LoggerFactory.getLogger(GraphEngineStationResolver.class);
    private static final ParameterizedTypeReference<Map<String, Integer>> MAP_TYPE =
            new ParameterizedTypeReference<>() { };

    private final WebClient client;
    private final long refreshMs;
    private volatile Map<String, Integer> stopToIndex = Map.of();
    private Disposable refreshLoop;

    public GraphEngineStationResolver(WebClient.Builder builder,
                                      @Value("${rail.graph-engine.url:http://localhost:8091}") String graphUrl,
                                      @Value("${rail.graph-engine.stopmap-refresh-ms:300000}") long refreshMs) {
        this.client = builder.baseUrl(graphUrl).build();
        this.refreshMs = refreshMs;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        load().subscribe();
        refreshLoop = Flux.interval(Duration.ofMillis(refreshMs), Duration.ofMillis(refreshMs))
                .concatMap(t -> load())
                .subscribe();
    }

    private Mono<Void> load() {
        return client.get().uri("/graph/stopmap").retrieve()
                .bodyToMono(MAP_TYPE)
                .timeout(Duration.ofSeconds(5))
                .doOnNext(m -> {
                    if (m != null && !m.isEmpty()) {
                        stopToIndex = m;
                        log.info("loaded {} stop_id → index mappings from graph engine", m.size());
                    }
                })
                .onErrorResume(e -> {
                    log.warn("stopmap refresh failed ({}); keeping {} cached mappings",
                            e.toString(), stopToIndex.size());
                    return Mono.empty();
                })
                .then();
    }

    @Override
    public Integer resolve(String stopId) {
        if (stopId == null || stopId.isBlank()) {
            return null;
        }
        Integer idx = stopToIndex.get(stopId);
        if (idx != null) {
            return idx;
        }
        int colon = stopId.lastIndexOf(':');
        if (colon >= 0 && colon < stopId.length() - 1) {
            return stopToIndex.get(stopId.substring(colon + 1));
        }
        return null;
    }

    public int size() {
        return stopToIndex.size();
    }

    /** Seed the cache directly — used by tests to exercise {@link #resolve} without HTTP. */
    void seed(Map<String, Integer> map) {
        this.stopToIndex = Map.copyOf(map);
    }

    @jakarta.annotation.PreDestroy
    void stop() {
        if (refreshLoop != null && !refreshLoop.isDisposed()) {
            refreshLoop.dispose();
        }
    }
}
