package com.rail.platform.graph.infrastructure.web;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rail.platform.graph.application.SpectralGraphService;
import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.SpectralMatrix;
import com.rail.platform.graph.domain.port.in.PlanJourneyUseCase;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

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

    public GraphMatrixController(TopologyRepository repository, StationSeriation seriation,
                                 PlanJourneyUseCase planner, SpectralGraphService spectral) {
        this.repository = repository;
        this.seriation = seriation;
        this.planner = planner;
        this.spectral = spectral;
    }

    @GetMapping("/matrix")
    public Mono<ResponseEntity<MatrixDto>> matrix() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            return t == null ? ResponseEntity.<MatrixDto>status(503).build()
                    : ResponseEntity.ok(toMatrix(t));
        });
    }

    @GetMapping("/stats")
    public Mono<ResponseEntity<Map<String, Object>>> stats() {
        return Mono.fromSupplier(() -> {
            RailTopology t = repository.current();
            if (t == null) {
                return ResponseEntity.status(503).build();
            }
            List<Map<String, Object>> hubs = IntStream.range(0, t.stationCount())
                    .boxed()
                    .sorted(Comparator.comparingDouble((Integer i) -> -t.pagerank(i)))
                    .limit(10)
                    .map(i -> {
                        Map<String, Object> h = new LinkedHashMap<>();
                        h.put("name", t.stationName(i));
                        h.put("pagerank", t.pagerank(i));
                        return h;
                    })
                    .collect(Collectors.toList());
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("graphVersion", t.graphVersion());
            body.put("stationCount", t.stationCount());
            body.put("edgeCount", t.edgeCount());
            body.put("routeCount", t.routeCount());
            body.put("topHubs", hubs);
            return ResponseEntity.ok(body);
        });
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
