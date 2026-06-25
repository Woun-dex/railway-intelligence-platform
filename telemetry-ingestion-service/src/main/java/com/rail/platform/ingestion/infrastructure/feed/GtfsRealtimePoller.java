package com.rail.platform.ingestion.infrastructure.feed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.rail.platform.ingestion.domain.model.RawTelemetry;
import com.rail.platform.ingestion.domain.model.TelemetryKind;
import com.rail.platform.ingestion.domain.port.in.IngestTelemetryUseCase;

import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Driving adapter that pulls real GTFS-Realtime feeds and drives them through the
 * same {@link IngestTelemetryUseCase} as the HTTP gateway — so normalization,
 * stop_id resolution, partitioning, and dead-lettering are all reused.
 *
 * <p>Disabled unless at least one feed URL is configured
 * ({@code rail.feed.gtfs-rt.trip-updates-url} / {@code ...vehicle-positions-url}).
 * Each tick fetches the binary {@code FeedMessage}, maps entities to canonical
 * position frames ({@link GtfsRealtimeMapper}), and ingests them as a bounded
 * stream. A failed poll is logged and retried on the next tick — a flaky feed
 * never takes the gateway down.
 */
@Component
public class GtfsRealtimePoller {

    private static final Logger log = LoggerFactory.getLogger(GtfsRealtimePoller.class);

    private final IngestTelemetryUseCase ingest;
    private final GtfsRealtimeMapper mapper;
    private final ObjectMapper json;
    private final WebClient client;

    private final String tripUpdatesUrl;
    private final String vehiclePositionsUrl;
    private final long pollMs;
    private final String apiKeyHeader;
    private final String apiKeyValue;

    private Disposable loop;

    public GtfsRealtimePoller(IngestTelemetryUseCase ingest, GtfsRealtimeMapper mapper, ObjectMapper json,
                              WebClient.Builder builder,
                              @Value("${rail.feed.gtfs-rt.trip-updates-url:}") String tripUpdatesUrl,
                              @Value("${rail.feed.gtfs-rt.vehicle-positions-url:}") String vehiclePositionsUrl,
                              @Value("${rail.feed.gtfs-rt.poll-interval-ms:30000}") long pollMs,
                              @Value("${rail.feed.gtfs-rt.api-key-header:}") String apiKeyHeader,
                              @Value("${rail.feed.gtfs-rt.api-key-value:}") String apiKeyValue) {
        this.ingest = ingest;
        this.mapper = mapper;
        this.json = json;
        // Large feeds: raise the in-memory decode cap for the binary body.
        this.client = builder.codecs(c -> c.defaultCodecs().maxInMemorySize(32 * 1024 * 1024)).build();
        this.tripUpdatesUrl = tripUpdatesUrl;
        this.vehiclePositionsUrl = vehiclePositionsUrl;
        this.pollMs = pollMs;
        this.apiKeyHeader = apiKeyHeader;
        this.apiKeyValue = apiKeyValue;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        List<String> urls = new ArrayList<>();
        if (!tripUpdatesUrl.isBlank()) urls.add(tripUpdatesUrl);
        if (!vehiclePositionsUrl.isBlank()) urls.add(vehiclePositionsUrl);
        if (urls.isEmpty()) {
            log.info("GTFS-RT poller disabled (no feed URL configured)");
            return;
        }
        log.info("GTFS-RT poller enabled: {} feed(s) every {}ms", urls.size(), pollMs);
        loop = Flux.interval(Duration.ZERO, Duration.ofMillis(pollMs))
                .concatMap(t -> Flux.fromIterable(urls).concatMap(this::pollOne))
                .subscribe();
    }

    private Mono<Void> pollOne(String url) {
        String source = url.equals(vehiclePositionsUrl) ? "GTFS-RT/VP" : "GTFS-RT/TU";
        return client.get().uri(url)
                .headers(h -> { if (!apiKeyHeader.isBlank()) h.set(apiKeyHeader, apiKeyValue); })
                .retrieve().bodyToMono(byte[].class)
                .timeout(Duration.ofSeconds(15))
                .flatMap(bytes -> ingestFrames(bytes, source))
                .onErrorResume(e -> {
                    log.warn("GTFS-RT poll failed for {}: {}", url, e.toString());
                    return Mono.empty();
                });
    }

    private Mono<Void> ingestFrames(byte[] bytes, String source) {
        FeedMessage msg;
        try {
            msg = FeedMessage.parseFrom(bytes);
        } catch (Exception e) {
            log.warn("GTFS-RT parse failed ({} bytes): {}", bytes.length, e.toString());
            return Mono.empty();
        }
        List<ObjectNode> frames = mapper.toPositionFrames(msg, source);
        if (frames.isEmpty()) {
            return Mono.empty();
        }
        log.debug("GTFS-RT {} → {} position frames", source, frames.size());
        Flux<RawTelemetry> raw = Flux.fromIterable(frames)
                .map(f -> RawTelemetry.of(TelemetryKind.POSITION, toBytes(f)));
        return ingest.ingestStream(raw);
    }

    private byte[] toBytes(ObjectNode node) {
        try {
            return json.writeValueAsBytes(node);
        } catch (Exception e) {
            return ("{\"_unserializable\":\"" + e.getMessage() + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    @PreDestroy
    void stop() {
        if (loop != null && !loop.isDisposed()) {
            loop.dispose();
        }
    }
}
