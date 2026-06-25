package com.rail.platform.graph.infrastructure.web;

import java.time.Duration;
import java.util.List;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rail.platform.graph.application.CriticalityState;
import com.rail.platform.schemas.display.CriticalityEvent;

import reactor.core.publisher.Flux;

/**
 * Read API over the streaming joiner's output: a live Criticality leaderboard
 * ({@code GET /graph/criticality/top}) and an SSE feed
 * ({@code GET /graph/criticality/stream}) the viewer can subscribe to. Both read
 * the in-heap {@link CriticalityState}, so they reflect the latest score per
 * station without replaying Kafka.
 */
@RestController
@RequestMapping("/graph/criticality")
public class CriticalityController {

    private final CriticalityState state;

    public CriticalityController(CriticalityState state) {
        this.state = state;
    }

    @GetMapping("/top")
    public List<CriticalityDto> top(@RequestParam(defaultValue = "15") int k) {
        return state.top(k).stream().map(CriticalityController::toDto).toList();
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<CriticalityDto>> stream() {
        Flux<ServerSentEvent<CriticalityDto>> data = state.stream()
                .map(CriticalityController::toDto)
                .map(d -> ServerSentEvent.builder(d).event("criticality").build());
        Flux<ServerSentEvent<CriticalityDto>> heartbeat = Flux.interval(Duration.ofSeconds(20))
                .map(i -> ServerSentEvent.<CriticalityDto>builder().comment("keep-alive").build());
        return Flux.merge(data, heartbeat);
    }

    private static CriticalityDto toDto(CriticalityEvent e) {
        return new CriticalityDto(
                e.getStationId(),
                e.getStationName(),
                e.getTripId(),
                round(e.getScore().getTotal()),
                round(e.getScore().getTImpact()),
                e.getScore().getPMiss() >= 0.5,
                round(e.getScore().getIEscalate()),
                round(e.getScore().getBetaHub()),
                e.getPredictionJoined(),
                e.getJoinLagMs(),
                e.getDecisionTimeMs());
    }

    private static double round(double v) {
        return Math.round(v * 1000.0) / 1000.0;
    }

    /** Flattened criticality row for the viewer. */
    public record CriticalityDto(int station, String stationName, String tripId,
                                 double cs, double tSurplusMin, boolean missedConnection,
                                 double escalation, double betaHub,
                                 boolean predictionJoined, long joinLagMs, long ts) {
    }
}
