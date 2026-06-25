package com.rail.platform.graph.infrastructure.topology;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.RailTopologyBuilder;

/**
 * Builds a {@link RailTopology} from a real GTFS feed (directory or {@code .zip}).
 *
 * <p>Pipeline: read {@code stops/routes/trips/stop_times/transfers.txt}, filter
 * routes to the configured {@code route_type}s (default rail), collapse platforms
 * to stations via {@code parent_station}, group trips into RAPTOR route-patterns,
 * derive the directed running graph from consecutive stop-times, and derive slack
 * via {@link SlackModel}. The hub vector is left uniform here and computed by the
 * hub-ranking step.
 */
@Component
public class GtfsTopologyLoader {

    private static final Logger log = LoggerFactory.getLogger(GtfsTopologyLoader.class);

    private final ResourceLoader resourceLoader;
    private final SlackModel slackModel;
    private final String defaultLocation;
    private final Set<Integer> defaultRouteTypes;
    private final Set<String> lineAllowList;
    private final int transferDefaultSec;

    public GtfsTopologyLoader(ResourceLoader resourceLoader, SlackModel slackModel,
                              @Value("${rail.graph.gtfs.path}") String defaultLocation,
                              @Value("${rail.graph.gtfs.route-types:2}") String routeTypes,
                              @Value("${rail.graph.gtfs.lines:A,B,C,D,E,H,J,K,L,N,P,R,U,V}") String lines,
                              @Value("${rail.graph.slack.transfer-default-sec:120}") int transferDefaultSec) {
        this.resourceLoader = resourceLoader;
        this.slackModel = slackModel;
        this.defaultLocation = defaultLocation;
        this.defaultRouteTypes = parseRouteTypes(routeTypes);
        this.lineAllowList = parseLines(lines);
        this.transferDefaultSec = transferDefaultSec;
    }

    public RailTopology load() {
        return load(defaultLocation, defaultRouteTypes);
    }

    public RailTopology load(String location, Set<Integer> routeTypes) {
        try (GtfsSource source = GtfsSource.resolve(location, resourceLoader)) {
            return build(source, location, routeTypes);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load GTFS from " + location, e);
        }
    }

    // -----------------------------------------------------------------------

    private record StopRow(String name, double lat, double lon, int locationType, String parent) {
    }

    private record RouteRow(int type, String lineName) {
    }

    private static final class StopTime {
        final int seq;
        final String stopId;
        final int arrSec;
        final int depSec;

        StopTime(int seq, String stopId, int arrSec, int depSec) {
            this.seq = seq;
            this.stopId = stopId;
            this.arrSec = arrSec;
            this.depSec = depSec;
        }
    }

    private static final class Stat {
        int min = Integer.MAX_VALUE;
        long sum;
        int count;

        void add(int v) {
            if (v < min) {
                min = v;
            }
            sum += v;
            count++;
        }
    }

    private RailTopology build(GtfsSource source, String location, Set<Integer> routeTypes) throws IOException {
        // ---- stops -> resolve platforms to stations ------------------------
        Map<String, StopRow> stops = readStops(source);
        log.info("GTFS stops.txt: {} entries read", stops.size());
        RailTopologyBuilder b = new RailTopologyBuilder();
        Map<String, Integer> stationIdx = new HashMap<>(); // gtfs stop_id (any level) -> station index

        // Stations are registered lazily as they are encountered in filtered trips,
        // so we don't load the 15,000+ bus/tram stop areas present in the full IDFM feed.

        // ---- routes filtered to rail/RER -----------------------------------
        Map<String, RouteRow> routes = readRoutes(source, routeTypes);
        log.info("GTFS routes: {} kept (route_types={})", routes.size(), routeTypes);
        // ---- trips on those routes -----------------------------------------
        Map<String, String> tripShape = new HashMap<>();                      // trip_id -> shape_id
        Map<String, String> tripRoute = readTrips(source, routes.keySet(), tripShape); // trip_id -> route_id
        log.info("GTFS trips: {} kept for {} routes ({} with shapes)",
                tripRoute.size(), routes.size(), tripShape.size());

        // ---- stop_times grouped by trip ------------------------------------
        Map<String, List<StopTime>> byTrip = readStopTimes(source, tripRoute.keySet());
        log.info("GTFS stop_times: {} trips with stop sequences loaded", byTrip.size());

        // ---- shapes for kept trips (real track geometry, optional) ---------
        Set<String> neededShapes = new java.util.HashSet<>();
        for (String tripId : byTrip.keySet()) {
            String sh = tripShape.get(tripId);
            if (sh != null) {
                neededShapes.add(sh);
            }
        }
        Map<String, double[][]> shapes = readShapes(source, neededShapes);
        log.info("GTFS shapes.txt: {} polylines loaded for {} referenced shapes",
                shapes.size(), neededShapes.size());
        Set<String> shapesProjected = new java.util.HashSet<>();
        Map<Long, float[]> segmentGeom = new LinkedHashMap<>();

        Map<Long, Stat> segStats = new HashMap<>();
        Map<Integer, Stat> dwellStats = new HashMap<>();

        for (Map.Entry<String, List<StopTime>> e : byTrip.entrySet()) {
            List<StopTime> times = e.getValue();
            if (times.size() < 2) {
                continue;
            }
            times.sort((x, y) -> Integer.compare(x.seq, y.seq));
            fillMissingTimes(times);

            String routeId = tripRoute.get(e.getKey());
            String lineName = routes.get(routeId).lineName();

            int n = times.size();
            int[] seq = new int[n];
            int[] arr = new int[n];
            int[] dep = new int[n];
            for (int i = 0; i < n; i++) {
                StopTime st = times.get(i);
                int idx = stationFor(st.stopId, stops, b, stationIdx);
                seq[i] = idx;
                arr[i] = st.arrSec;
                dep[i] = st.depSec;
                b.line(idx, lineName);
            }
            b.trip(lineName, seq, arr, dep);

            // Real track geometry: project this trip's stations onto its shape and
            // slice the polyline per consecutive station pair. Done once per shape.
            String shapeId = tripShape.get(e.getKey());
            if (shapeId != null && shapesProjected.add(shapeId)) {
                double[][] poly = shapes.get(shapeId);
                if (poly != null && poly.length >= 2) {
                    projectSegments(seq, b, poly, segmentGeom);
                }
            }

            // Segment running time and interior dwell statistics.
            for (int i = 0; i < n - 1; i++) {
                int run = arr[i + 1] - dep[i];
                if (run >= 0 && run < 6 * 3600) {
                    segStats.computeIfAbsent(key(seq[i], seq[i + 1]), k -> new Stat()).add(run);
                }
                if (i > 0) {
                    int dwell = dep[i] - arr[i];
                    if (dwell >= 0 && dwell < 3600) {
                        dwellStats.computeIfAbsent(seq[i], k -> new Stat()).add(dwell);
                    }
                }
            }
        }

        // ---- flush segments + dwell slack ----------------------------------
        for (Map.Entry<Long, Stat> e : segStats.entrySet()) {
            int from = (int) (e.getKey() >> 32);
            int to = e.getKey().intValue();
            Stat s = e.getValue();
            b.segment(from, to, s.min, slackModel.runSlack(s.min, s.sum, s.count));
        }
        for (Map.Entry<Integer, Stat> e : dwellStats.entrySet()) {
            Stat s = e.getValue();
            b.dwellSlack(e.getKey(), slackModel.dwellSlack(s.min, s.sum, s.count));
        }

        // ---- transfers -----------------------------------------------------
        readTransfers(source, stops, b, stationIdx);

        b.geometry(segmentGeom);
        log.info("GTFS geometry: {} directed station segments carry real track curvature", segmentGeom.size());

        b.graphVersion("g-" + Instant.now().getEpochSecond());

        RailTopology topology = b.build();
        log.info("Loaded GTFS from {} -> {} stations, {} edges, {} routes",
                location, topology.stationCount(), topology.edgeCount(), topology.routeCount());
        return topology;
    }

    // -----------------------------------------------------------------------

    private Map<String, StopRow> readStops(GtfsSource source) throws IOException {
        Map<String, StopRow> stops = new LinkedHashMap<>();
        try (InputStream in = require(source, "stops.txt")) {
            GtfsCsvReader.forEach(in, r -> {
                String id = r.get("stop_id");
                if (id.isEmpty()) {
                    return;
                }
                stops.put(id, new StopRow(
                        r.get("stop_name"),
                        parseDouble(r.get("stop_lat")),
                        parseDouble(r.get("stop_lon")),
                        r.getInt("location_type", 0),
                        r.get("parent_station")));
            });
        }
        return stops;
    }

    private Map<String, RouteRow> readRoutes(GtfsSource source, Set<Integer> routeTypes) throws IOException {
        Map<String, RouteRow> routes = new LinkedHashMap<>();
        try (InputStream in = require(source, "routes.txt")) {
            GtfsCsvReader.forEach(in, r -> {
                int type = r.getInt("route_type", -1);
                if (!routeTypes.contains(type)) {
                    return;
                }
                String shortName = r.get("route_short_name");
                // Line allow-list: in the full IDFM feed, route_type=2 (Rail) also
                // includes TER/Intercités lines that sprawl across France. Keeping
                // only the Transilien/RER commercial codes isolates the ~392-station
                // Île-de-France network. Empty allow-list = keep every rail route.
                if (!lineAllowList.isEmpty()
                        && !lineAllowList.contains(shortName.trim().toUpperCase(Locale.ROOT))) {
                    return;
                }
                String name = shortName;
                if (name.isEmpty()) {
                    name = r.get("route_long_name");
                }
                if (name.isEmpty()) {
                    name = r.get("route_id");
                }
                routes.put(r.get("route_id"), new RouteRow(type, name));
            });
        }
        return routes;
    }

    private Map<String, String> readTrips(GtfsSource source, Set<String> keptRoutes,
                                          Map<String, String> tripShapeOut) throws IOException {
        Map<String, String> tripRoute = new LinkedHashMap<>();
        try (InputStream in = require(source, "trips.txt")) {
            GtfsCsvReader.forEach(in, r -> {
                String routeId = r.get("route_id");
                if (keptRoutes.contains(routeId)) {
                    String tripId = r.get("trip_id");
                    tripRoute.put(tripId, routeId);
                    String shapeId = r.get("shape_id");
                    if (!shapeId.isEmpty()) {
                        tripShapeOut.put(tripId, shapeId);
                    }
                }
            });
        }
        return tripRoute;
    }

    /**
     * Reads {@code shapes.txt} for the requested shape ids into ordered
     * {@code [lon, lat]} polylines. Returns an empty map when the feed has no
     * shapes — geometry is optional and consumers fall back to straight segments.
     */
    private Map<String, double[][]> readShapes(GtfsSource source, Set<String> needed) throws IOException {
        if (needed.isEmpty() || !source.has("shapes.txt")) {
            return Map.of();
        }
        Map<String, List<double[]>> acc = new HashMap<>();
        try (InputStream in = source.open("shapes.txt")) {
            if (in == null) {
                return Map.of();
            }
            GtfsCsvReader.forEach(in, r -> {
                String id = r.get("shape_id");
                if (id.isEmpty() || !needed.contains(id)) {
                    return;
                }
                double lat = parseDouble(r.get("shape_pt_lat"));
                double lon = parseDouble(r.get("shape_pt_lon"));
                int seq = r.getInt("shape_pt_sequence", 0);
                // [lon, lat, sequence] — sorted by sequence after collection.
                acc.computeIfAbsent(id, k -> new ArrayList<>()).add(new double[] {lon, lat, seq});
            });
        }
        Map<String, double[][]> shapes = new HashMap<>();
        for (Map.Entry<String, List<double[]>> e : acc.entrySet()) {
            List<double[]> pts = e.getValue();
            pts.sort((a, b) -> Double.compare(a[2], b[2]));
            double[][] poly = new double[pts.size()][];
            for (int i = 0; i < pts.size(); i++) {
                poly[i] = new double[] {pts.get(i)[0], pts.get(i)[1]};
            }
            shapes.put(e.getKey(), poly);
        }
        return shapes;
    }

    private Map<String, List<StopTime>> readStopTimes(GtfsSource source, Set<String> keptTrips) throws IOException {
        // LinkedHashMap: trips keep stop_times file order, so station registration
        // (and therefore station indices) are deterministic across loads.
        Map<String, List<StopTime>> byTrip = new LinkedHashMap<>();
        try (InputStream in = require(source, "stop_times.txt")) {
            long[] rowCount = {0};
            GtfsCsvReader.forEach(in, r -> {
                rowCount[0]++;
                if (rowCount[0] % 500_000 == 0) {
                    log.info("  stop_times.txt: {}K rows scanned, {} trips kept so far",
                            rowCount[0] / 1000, byTrip.size());
                }
                String tripId = r.get("trip_id");
                if (!keptTrips.contains(tripId)) {
                    return;
                }
                int arr = parseTime(r.get("arrival_time"));
                int dep = parseTime(r.get("departure_time"));
                byTrip.computeIfAbsent(tripId, k -> new ArrayList<>())
                        .add(new StopTime(r.getInt("stop_sequence", 0), r.get("stop_id"), arr, dep));
            });
            log.info("  stop_times.txt: {} total rows scanned", rowCount[0]);
        }
        return byTrip;
    }

    private void readTransfers(GtfsSource source, Map<String, StopRow> stops,
                              RailTopologyBuilder b, Map<String, Integer> stationIdx) throws IOException {
        if (!source.has("transfers.txt")) {
            return;
        }
        try (InputStream in = source.open("transfers.txt")) {
            if (in == null) {
                return;
            }
            GtfsCsvReader.forEach(in, r -> {
                String from = r.get("from_stop_id");
                String to = r.get("to_stop_id");
                if (from.isEmpty() || to.isEmpty()) {
                    return;
                }
                // Only connect stations already discovered from rail trips. The full
                // IDFM transfers.txt links every mode's stops; resolving them through
                // stationFor() would register thousands of bus/metro/tram stations
                // that no kept rail trip ever visits.
                Integer fi = existingStation(from, stops, stationIdx);
                Integer ti = existingStation(to, stops, stationIdx);
                if (fi == null || ti == null || fi.equals(ti)) {
                    return;
                }
                int min = r.getInt("min_transfer_time", transferDefaultSec);
                b.transfer(fi, ti, min, slackModel.transferSlack(min));
            });
        }
    }

    /**
     * Resolves a (possibly platform-level) stop to its station index, registering
     * it on demand. Follows the {@code parent_station} chain if present. The IDFM
     * feed sometimes references parent stops that themselves have a parent; this
     * method walks one level (platform → station) which is the GTFS convention.
     */
    private int stationFor(String stopId, Map<String, StopRow> stops,
                           RailTopologyBuilder b, Map<String, Integer> stationIdx) {
        Integer cached = stationIdx.get(stopId);
        if (cached != null) {
            return cached;
        }
        StopRow row = stops.get(stopId);
        String stationStopId = stopId;
        StopRow stationRow = row;
        // Follow parent_station if the parent exists in the stops table.
        if (row != null && !row.parent().isEmpty()) {
            StopRow parentRow = stops.get(row.parent());
            if (parentRow != null) {
                stationStopId = row.parent();
                stationRow = parentRow;
            }
        }
        Integer existing = stationIdx.get(stationStopId);
        int idx;
        if (existing != null) {
            idx = existing;
        } else {
            String name = stationRow != null ? stationRow.name() : stopId;
            double lat = stationRow != null ? stationRow.lat() : 0.0;
            double lon = stationRow != null ? stationRow.lon() : 0.0;
            idx = b.station(stationStopId, name, lat, lon);
            stationIdx.put(stationStopId, idx);
        }
        stationIdx.put(stopId, idx);
        return idx;
    }

    /**
     * Resolves a stop to an <em>already-registered</em> station index without
     * creating a new one. Checks the stop id directly, then its parent station.
     * Returns {@code null} if neither has been seen — used by the transfers pass
     * so it never introduces stations that no kept trip visits.
     */
    private static Integer existingStation(String stopId, Map<String, StopRow> stops,
                                           Map<String, Integer> stationIdx) {
        Integer direct = stationIdx.get(stopId);
        if (direct != null) {
            return direct;
        }
        StopRow row = stops.get(stopId);
        if (row != null && !row.parent().isEmpty()) {
            return stationIdx.get(row.parent());
        }
        return null;
    }

    private static InputStream require(GtfsSource source, String name) throws IOException {
        InputStream in = source.open(name);
        if (in == null) {
            throw new IOException("GTFS feed is missing required file: " + name);
        }
        return in;
    }

    /** Fills empty arrival/departure with the neighbour value, then carries forward. */
    private static void fillMissingTimes(List<StopTime> times) {
        int last = -1;
        for (int i = 0; i < times.size(); i++) {
            StopTime st = times.get(i);
            int arr = st.arrSec >= 0 ? st.arrSec : st.depSec;
            int dep = st.depSec >= 0 ? st.depSec : arr;
            if (arr < 0) {
                arr = last >= 0 ? last : 0;
            }
            if (dep < 0) {
                dep = arr;
            }
            times.set(i, new StopTime(st.seq, st.stopId, arr, dep));
            last = dep;
        }
    }

    static Set<Integer> parseRouteTypes(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
    }

    /** Parse the line allow-list (route_short_name codes), upper-cased. */
    static Set<String> parseLines(String csv) {
        if (csv == null) {
            return Set.of();
        }
        return Arrays.stream(csv.split(","))
                .map(s -> s.trim().toUpperCase(Locale.ROOT))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    private static long key(int from, int to) {
        return (((long) from) << 32) | (to & 0xffffffffL);
    }

    /**
     * Projects a trip's ordered stations onto its shape polyline and stores the
     * sliced track curve for each consecutive station pair (first-wins). The
     * shape is oriented to the direction of travel first (feeds often store one
     * shape used in both directions). Pairs whose stations land on adjacent shape
     * points carry no interior curve and are left to the straight-line fallback.
     */
    private static void projectSegments(int[] seq, RailTopologyBuilder b,
                                        double[][] poly, Map<Long, float[]> out) {
        int n = seq.length;
        int m = poly.length;

        // Orient the shape to travel direction using the unconstrained endpoints.
        int pFirst = nearest(poly, b.stationLon(seq[0]), b.stationLat(seq[0]), 0);
        int pLast = nearest(poly, b.stationLon(seq[n - 1]), b.stationLat(seq[n - 1]), 0);
        if (pFirst > pLast) {
            double[][] rev = new double[m][];
            for (int i = 0; i < m; i++) {
                rev[i] = poly[m - 1 - i];
            }
            poly = rev;
        }

        // Monotonic forward projection so slices advance along the track.
        int[] proj = new int[n];
        int last = 0;
        for (int i = 0; i < n; i++) {
            proj[i] = nearest(poly, b.stationLon(seq[i]), b.stationLat(seq[i]), last);
            last = proj[i];
        }

        for (int i = 0; i < n - 1; i++) {
            int a = proj[i];
            int c = proj[i + 1];
            if (c - a < 2) {
                continue; // no interior shape points -> straight segment suffices
            }
            long k = key(seq[i], seq[i + 1]);
            if (out.containsKey(k)) {
                continue;
            }
            int interior = c - a - 1;
            float[] flat = new float[(interior + 2) * 2];
            int w = 0;
            flat[w++] = (float) b.stationLon(seq[i]);
            flat[w++] = (float) b.stationLat(seq[i]);
            for (int p = a + 1; p < c; p++) {
                flat[w++] = (float) poly[p][0];
                flat[w++] = (float) poly[p][1];
            }
            flat[w++] = (float) b.stationLon(seq[i + 1]);
            flat[w] = (float) b.stationLat(seq[i + 1]);
            out.put(k, flat);
        }
    }

    /** Index of the shape point nearest {@code (lon, lat)}, scanning from {@code from}. */
    private static int nearest(double[][] poly, double lon, double lat, int from) {
        int best = from;
        double bestD = Double.MAX_VALUE;
        for (int p = from; p < poly.length; p++) {
            double dlo = poly[p][0] - lon;
            double dla = poly[p][1] - lat;
            double dd = dlo * dlo + dla * dla;
            if (dd < bestD) {
                bestD = dd;
                best = p;
            }
        }
        return best;
    }

    private static int parseTime(String hhmmss) {
        if (hhmmss == null || hhmmss.isEmpty()) {
            return -1;
        }
        String[] p = hhmmss.split(":");
        if (p.length < 2) {
            return -1;
        }
        try {
            int h = Integer.parseInt(p[0].trim());
            int m = Integer.parseInt(p[1].trim());
            int s = p.length > 2 ? Integer.parseInt(p[2].trim()) : 0;
            return h * 3600 + m * 60 + s;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static double parseDouble(String v) {
        if (v == null || v.isEmpty()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
