package com.rail.platform.graph.infrastructure.web;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rail.platform.graph.application.HubRankingCoordinator;
import com.rail.platform.graph.application.OperationalState;
import com.rail.platform.graph.application.SpectralGraphService;
import com.rail.platform.graph.domain.model.DelaySource;
import com.rail.platform.graph.domain.model.JourneyPath;
import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.NetworkGeometry;
import com.rail.platform.graph.domain.model.PropagationResult;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.SpectralMatrix;
import com.rail.platform.graph.domain.port.in.PlanJourneyUseCase;
import com.rail.platform.graph.domain.port.in.PropagateDelayUseCase;
import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.graph.infrastructure.messaging.DisplayStreamBridge;
import com.rail.platform.schemas.display.DisplayMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Exposes the in-memory network as a matrix ({@code GET /graph/matrix}) for the
 * viewer, a summary ({@code GET /graph/stats}), and a RAPTOR query endpoint
 * ({@code GET /graph/plan}) so Engine 1b is demoable from the browser. All
 * responses reflect the live topology held in heap by the Hazelcast repository.
 */
@RestController
@RequestMapping("/graph")
public class GraphMatrixController {

    private final TopologyRepository repository;
    private final StationSeriation seriation;
    private final PlanJourneyUseCase planner;
    private final SpectralGraphService spectral;
    private final PropagateDelayUseCase propagator;
    private final DisplayStreamBridge displayStream;
    private final OperationalState operationalState;
    private final HubRankingCoordinator hubCoordinator;

    /** Public Mapbox token, supplied via the MAPBOX_TOKEN env var; never hard-coded in source. */
    @Value("${rail.mapbox.token:}")
    private String mapboxToken;

    public GraphMatrixController(TopologyRepository repository, StationSeriation seriation,
                                 PlanJourneyUseCase planner, SpectralGraphService spectral,
                                 PropagateDelayUseCase propagator, DisplayStreamBridge displayStream,
                                 OperationalState operationalState, HubRankingCoordinator hubCoordinator) {
        this.repository = repository;
        this.seriation = seriation;
        this.planner = planner;
        this.spectral = spectral;
        this.propagator = propagator;
        this.displayStream = displayStream;
        this.operationalState = operationalState;
        this.hubCoordinator = hubCoordinator;
    }

    /**
     * Viewer config: serves the Mapbox token from the {@code MAPBOX_TOKEN} env var
     * so it lives in the environment, not in committed source. Empty when unset —
     * the viewer then falls back to a {@code ?token=} param or {@code localStorage}.
     */
    @GetMapping("/config")
    public Mono<Map<String, String>> config() {
        return Mono.just(Map.of("mapboxToken", mapboxToken == null ? "" : mapboxToken));
    }

    @GetMapping("/matrix")
    public Mono<ResponseEntity<MatrixDto>> matrix() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            return t == null ? ResponseEntity.<MatrixDto>status(503).build()
                    : ResponseEntity.ok(toMatrix(t));
        });
    }

    /**
     * GTFS {@code stop_id} → internal station index map. Real-time feeds (SIRI-ET /
     * GTFS-RT) address stops by their GTFS {@code stop_id} string, but the engine
     * (and the STGCN spatial operator) index stations by a dense integer. The
     * ingestion gateway loads this map to resolve incoming feed identifiers before
     * publishing, so a real feed addresses the right node.
     */
    @GetMapping("/stopmap")
    public Mono<ResponseEntity<Map<String, Integer>>> stopMap() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<Map<String, Integer>>status(503).build();
            }
            Map<String, Integer> map = new LinkedHashMap<>();
            for (int i = 0; i < t.stationCount(); i++) {
                String sid = t.stopId(i);
                if (sid != null && !sid.isBlank()) {
                    map.putIfAbsent(sid, i);
                }
            }
            return ResponseEntity.ok(map);
        });
    }

    @GetMapping("/stats")
    public Mono<ResponseEntity<Map<String, Object>>> stats() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.status(503).build();
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("graphVersion", t.graphVersion());
            body.put("stationCount", t.stationCount());
            body.put("edgeCount", t.edgeCount());
            body.put("routeCount", t.routeCount());
            body.put("topHubs", topHubs(t, 10));
            return ResponseEntity.ok(body);
        });
    }

    private List<Map<String, Object>> topHubs(RailTopology t, int k) {
        return IntStream.range(0, t.stationCount())
                .boxed()
                .sorted(Comparator.comparingDouble((Integer i) -> -t.pagerank(i)))
                .limit(k)
                .map(i -> {
                    Map<String, Object> h = new LinkedHashMap<>();
                    h.put("index", i);
                    h.put("name", t.stationName(i));
                    h.put("pagerank", t.pagerank(i));
                    return h;
                })
                .collect(Collectors.toList());
    }

    /**
     * PCC control: apply an operational priority multiplier to a line ("canal")
     * or a station ({@code weight} 0 = closed/suppressed, 1 = normal/clear,
     * &gt;1 = boosted), then recompute the hub ranking immediately. Returns the
     * applied directive, the active overrides, and the new top hubs.
     */
    @PostMapping("/hub/pcc")
    public Mono<ResponseEntity<Map<String, Object>>> applyPcc(@RequestBody PccDirectiveDto d) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.status(503).build();
            }
            String target = d.target() == null ? "" : d.target().trim().toUpperCase();
            double weight = Math.max(0.0, d.weight());
            if ("LINE".equals(target)) {
                if (d.id() == null || d.id().isBlank()) {
                    return ResponseEntity.badRequest().build();
                }
                operationalState.setLine(d.id().trim(), weight);
            } else if ("STATION".equals(target)) {
                int idx;
                try {
                    idx = Integer.parseInt(d.id().trim());
                } catch (NumberFormatException | NullPointerException e) {
                    return ResponseEntity.badRequest().build();
                }
                if (!t.isStation(idx)) {
                    return ResponseEntity.badRequest().build();
                }
                operationalState.setStation(idx, weight);
            } else {
                return ResponseEntity.badRequest().build();
            }
            RailTopology updated = hubCoordinator.recompute(
                    "pcc:" + target + ":" + d.id() + "=" + weight);
            return ResponseEntity.ok(pccBody(updated == null ? t : updated, d));
        });
    }

    /** Lists the active PCC overrides and the current top hubs. */
    @GetMapping("/hub/pcc")
    public Mono<ResponseEntity<Map<String, Object>>> listPcc() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.status(503).build();
            }
            return ResponseEntity.ok(pccBody(t, null));
        });
    }

    /** Clears all PCC overrides and recomputes the (structural) hub ranking. */
    @DeleteMapping("/hub/pcc")
    public Mono<ResponseEntity<Map<String, Object>>> clearPcc() {
        return Mono.fromSupplier(() -> {
            operationalState.clear();
            RailTopology updated = hubCoordinator.recompute("pcc:clear");
            RailTopology t = updated != null ? updated : repository.current();
            if (t == null) {
                return ResponseEntity.status(503).build();
            }
            return ResponseEntity.ok(pccBody(t, null));
        });
    }

    private Map<String, Object> pccBody(RailTopology t, PccDirectiveDto applied) {
        Map<String, Object> body = new LinkedHashMap<>();
        if (applied != null) {
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("target", applied.target());
            a.put("id", applied.id());
            a.put("weight", applied.weight());
            a.put("reason", applied.reason());
            body.put("applied", a);
        }
        body.put("lineOverrides", operationalState.lines());
        body.put("stationOverrides", operationalState.stations());
        body.put("topHubs", topHubs(t, 10));
        return body;
    }

    public record PccDirectiveDto(String target, String id, double weight, String reason) {
    }

    @GetMapping("/plan")
    public Mono<ResponseEntity<JourneyPlan>> plan(@RequestParam int from,
                                                  @RequestParam int to,
                                                  @RequestParam(defaultValue = "25200") int departure) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<JourneyPlan>status(503).build();
            }
            if (!t.isStation(from) || !t.isStation(to)) {
                return ResponseEntity.<JourneyPlan>badRequest().build();
            }
            return ResponseEntity.ok(planner.plan(JourneyQuery.of(from, to, departure)));
        });
    }

    /**
     * Reconstructed earliest-arrival journey (leg-by-leg path) for the viewer to
     * draw on the map: each leg lists the stations it passes through, so the line
     * follows the real stop sequence (RAPTOR, not road routing).
     */
    @GetMapping("/journey")
    public Mono<ResponseEntity<JourneyPath>> journey(@RequestParam int from,
                                                     @RequestParam int to,
                                                     @RequestParam(defaultValue = "25200") int departure) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<JourneyPath>status(503).build();
            }
            if (!t.isStation(from) || !t.isStation(to)) {
                return ResponseEntity.<JourneyPath>badRequest().build();
            }
            return ResponseEntity.ok(planner.planPath(JourneyQuery.of(from, to, departure)));
        });
    }

    /**
     * Engine 1a cascade for the viewer: propagate a synthetic {@code delay}
     * (seconds) injected at station {@code from} and return every affected node
     * with its residual delay, absorbed slack, hop distance, and missed-connection
     * flag — so the browser can animate the ripple without going through Kafka.
     */
    @GetMapping("/propagate")
    public Mono<ResponseEntity<List<PropagationResult>>> propagate(
            @RequestParam int from,
            @RequestParam(defaultValue = "600") int delay) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<List<PropagationResult>>status(503).build();
            }
            if (!t.isStation(from)) {
                return ResponseEntity.<List<PropagationResult>>badRequest().build();
            }
            DelaySource src = new DelaySource(
                    "viz", from, delay, "viz-" + System.nanoTime(), System.currentTimeMillis());
            return ResponseEntity.ok(propagator.propagate(src));
        });
    }

    /**
     * Spectral view for the STGCN spatial layer. {@code form} = {@code scaled}
     * (Chebyshev-ready L̃, default), {@code laplacian} (normalized L), or
     * {@code adjacency} (renormalized GCN Â). Returned as a sparse COO matrix.
     */
    @GetMapping("/spectral")
    public Mono<ResponseEntity<SpectralMatrix>> spectral(@RequestParam(defaultValue = "scaled") String form) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<SpectralMatrix>status(503).build();
            }
            SpectralGraphService.Form f = switch (form.toLowerCase()) {
                case "adjacency" -> SpectralGraphService.Form.ADJACENCY;
                case "laplacian" -> SpectralGraphService.Form.LAPLACIAN;
                default -> SpectralGraphService.Form.SCALED;
            };
            return ResponseEntity.ok(spectral.compute(t, f));
        });
    }

    /**
     * Search stations by name (case-insensitive substring). Useful for the full
     * IDFM network where there are hundreds of stations and indices are dynamic.
     * Returns up to {@code limit} matches with their index, name, coordinates,
     * and served lines.
     */
    @GetMapping("/stations")
    public Mono<ResponseEntity<List<Map<String, Object>>>> searchStations(
            @RequestParam(required = false) String q,
            @RequestParam(defaultValue = "20") int limit) {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<List<Map<String, Object>>>status(503).build();
            }
            String query = (q == null || q.isBlank()) ? "" : q.toLowerCase();
            List<Map<String, Object>> results = new ArrayList<>();
            for (int i = 0; i < t.stationCount() && results.size() < limit; i++) {
                if (!query.isEmpty() && !t.stationName(i).toLowerCase().contains(query)) {
                    continue;
                }
                Map<String, Object> station = new LinkedHashMap<>();
                station.put("index", i);
                station.put("stopId", t.stopId(i));
                station.put("name", t.stationName(i));
                station.put("lat", t.lat(i));
                station.put("lon", t.lon(i));
                List<String> lines = new ArrayList<>();
                for (int li : t.linesOf(i)) {
                    lines.add(t.lineNames()[li]);
                }
                station.put("lines", lines);
                results.add(station);
            }
            return ResponseEntity.ok(results);
        });
    }

    /**
     * Live departure board as a Server-Sent-Events stream tapped off the
     * {@code rail.display} Kafka topic — the event-driven display feed. Each event
     * is one departure ("canal" = line, terminus, absolute departure epoch so the
     * client renders a live local-time countdown). Optional {@code station} filters
     * the stream server-side to one board. A periodic comment keeps the SSE alive.
     */
    @GetMapping(value = "/display/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<DisplayEventDto>> displayStream(@RequestParam(required = false) Integer station) {
        Flux<ServerSentEvent<DisplayEventDto>> data = displayStream.stream()
                .map(GraphMatrixController::toDisplayEvent)
                .filter(d -> station == null || d.station() == station)
                .map(d -> ServerSentEvent.builder(d).event("departure").build());
        Flux<ServerSentEvent<DisplayEventDto>> heartbeat = Flux.interval(Duration.ofSeconds(20))
                .map(i -> ServerSentEvent.<DisplayEventDto>builder().comment("keep-alive").build());
        return Flux.merge(data, heartbeat);
    }

    private static DisplayEventDto toDisplayEvent(DisplayMessage m) {
        int station = -1;
        String[] parts = m.getSubject().split("/"); // piv/idf/{station}/{line}
        if (parts.length >= 3) {
            try {
                station = Integer.parseInt(parts[2]);
            } catch (NumberFormatException ignore) {
                // leave station = -1 (won't match any station filter)
            }
        }
        return new DisplayEventDto(station, m.getTripId(), m.getHeadline(),
                m.getValidToMs(), m.getValidFromMs(), m.getDisplaySeq());
    }

    /**
     * Real track geometry: every directed station segment that carries a curved
     * polyline from GTFS {@code shapes.txt}. The viewer overlays these so the base
     * network follows the true track instead of straight chords.
     */
    @GetMapping("/geometry")
    public Mono<ResponseEntity<GeometryDto>> geometry() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.<GeometryDto>status(503).build();
            }
            NetworkGeometry g = t.geometry();
            List<SegmentDto> segments = new ArrayList<>(g.segmentCount());
            for (Map.Entry<Long, float[]> e : g.segments().entrySet()) {
                long k = e.getKey();
                int from = (int) (k >> 32);
                int to = (int) k;
                float[] flat = e.getValue();
                double[][] path = new double[flat.length / 2][];
                for (int i = 0; i < path.length; i++) {
                    path[i] = new double[] {flat[2 * i], flat[2 * i + 1]};
                }
                segments.add(new SegmentDto(from, to, path));
            }
            return ResponseEntity.ok(new GeometryDto(segments.size(), segments));
        });
    }

    public record DisplayEventDto(int station, String line, String destination,
                                  long depMs, long cycleMs, long seq) {
    }

    public record GeometryDto(int count, List<SegmentDto> segments) {
    }

    public record SegmentDto(int from, int to, double[][] path) {
    }

    private MatrixDto toMatrix(RailTopology t) {
        int n = t.stationCount();
        List<MatrixDto.Station> stations = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            List<String> lines = new ArrayList<>();
            for (int li : t.linesOf(i)) {
                lines.add(t.lineNames()[li]);
            }
            stations.add(new MatrixDto.Station(i, t.stopId(i), t.stationName(i),
                    t.lat(i), t.lon(i), lines, t.pagerank(i)));
        }
        List<int[]> edges = new ArrayList<>(t.edgeCount());
        for (int i = 0; i < n; i++) {
            for (int e = t.adjBegin(i); e < t.adjEnd(i); e++) {
                edges.add(new int[]{i, t.adjTarget(e), t.adjWeightSec(e)});
            }
        }
        return new MatrixDto(t.graphVersion(), n, stations, edges, seriation.order(t));
    }
}
