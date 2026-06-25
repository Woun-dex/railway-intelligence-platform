package com.rail.platform.graph.domain.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Mutable accumulator that assembles a {@link RailTopology} into its final CSR
 * form. It is the single construction path used by both the GTFS loader
 * (production) and the test fixtures (deterministic mock networks), so the flat
 * layout is built and validated in exactly one place.
 *
 * <p>Not thread-safe; build a topology on one thread, then publish the immutable
 * result.
 */
public final class RailTopologyBuilder {

    // ---- Stations ----------------------------------------------------------
    private final Map<String, Integer> stationIndex = new LinkedHashMap<>();
    private final List<String> stopIds = new ArrayList<>();
    private final List<String> names = new ArrayList<>();
    private final List<Double> lats = new ArrayList<>();
    private final List<Double> lons = new ArrayList<>();
    private final List<LinkedHashSet<Integer>> stationLineSets = new ArrayList<>();

    // ---- Lines -------------------------------------------------------------
    private final Map<String, Integer> lineIndex = new LinkedHashMap<>();
    private final List<String> lineNames = new ArrayList<>();

    // ---- Adjacency / transfers (keyed (from<<32)|to, sorted) ---------------
    private final TreeMap<Long, int[]> edges = new TreeMap<>();      // -> {runtimeSec, runSlackSec}
    private final TreeMap<Long, int[]> transfers = new TreeMap<>();  // -> {minSec, slackSec}
    private final Map<Integer, Integer> dwellSlack = new LinkedHashMap<>();

    // ---- Routes (RAPTOR) ---------------------------------------------------
    private final Map<String, RouteAccum> routes = new LinkedHashMap<>();

    // ---- Display geometry (directed station-pair key -> [lon,lat,...]) ------
    private Map<Long, float[]> segmentGeom = new LinkedHashMap<>();

    private String graphVersion = "g-dev";

    private static long key(int from, int to) {
        return (((long) from) << 32) | (to & 0xffffffffL);
    }

    private static final class RouteAccum {
        final int[] pattern;
        final String line;
        final List<int[]> arr = new ArrayList<>();
        final List<int[]> dep = new ArrayList<>();
        RouteAccum(int[] pattern, String line) { this.pattern = pattern; this.line = line; }
    }

    /** Adds a station (idempotent by {@code stopId}); returns its index. */
    public int station(String stopId, String name, double lat, double lon) {
        Integer existing = stationIndex.get(stopId);
        if (existing != null) {
            return existing;
        }
        int idx = stopIds.size();
        stationIndex.put(stopId, idx);
        stopIds.add(stopId);
        names.add(name);
        lats.add(lat);
        lons.add(lon);
        stationLineSets.add(new LinkedHashSet<>());
        return idx;
    }

    /** Tags a station as served by a commercial line. */
    public RailTopologyBuilder line(int station, String lineName) {
        stationLineSets.get(station).add(lineIdx(lineName));
        return this;
    }

    /** Registers a commercial line (idempotent); returns its index. */
    private int lineIdx(String lineName) {
        return lineIndex.computeIfAbsent(lineName, k -> {
            lineNames.add(k);
            return lineNames.size() - 1;
        });
    }

    /** Station latitude by index (construction-time accessor for geometry projection). */
    public double stationLat(int idx) { return lats.get(idx); }

    /** Station longitude by index (construction-time accessor for geometry projection). */
    public double stationLon(int idx) { return lons.get(idx); }

    /** Supplies the directed station-pair track polylines built from {@code shapes.txt}. */
    public RailTopologyBuilder geometry(Map<Long, float[]> segments) {
        this.segmentGeom = segments;
        return this;
    }

    /** Adds/updates a directed running edge; on a duplicate keeps the faster run. */
    public RailTopologyBuilder segment(int from, int to, int runtimeSec, int runSlackSec) {
        edges.merge(key(from, to), new int[]{runtimeSec, Math.max(0, runSlackSec)}, (a, b) ->
                b[0] < a[0] ? b : a);
        return this;
    }

    public RailTopologyBuilder dwellSlack(int station, int slackSec) {
        dwellSlack.put(station, Math.max(0, slackSec));
        return this;
    }

    public RailTopologyBuilder transfer(int from, int to, int minSec, int slackSec) {
        transfers.put(key(from, to), new int[]{Math.max(0, minSec), Math.max(0, slackSec)});
        return this;
    }

    /**
     * Adds a trip. Trips that share an identical ordered {@code stationSeq} are
     * grouped into one RAPTOR route. {@code arrSec}/{@code depSec} are
     * seconds-of-day and must match {@code stationSeq} in length.
     */
    public RailTopologyBuilder trip(String lineName, int[] stationSeq, int[] arrSec, int[] depSec) {
        if (stationSeq.length != arrSec.length || stationSeq.length != depSec.length) {
            throw new IllegalArgumentException("trip arrays must share length");
        }
        lineIdx(lineName);
        String patternKey = lineName + "|" + Arrays.toString(stationSeq);
        RouteAccum r = routes.computeIfAbsent(patternKey, k -> new RouteAccum(stationSeq.clone(), lineName));
        r.arr.add(arrSec.clone());
        r.dep.add(depSec.clone());
        return this;
    }

    public RailTopologyBuilder graphVersion(String v) {
        this.graphVersion = v;
        return this;
    }

    public int stationCount() { return stopIds.size(); }

    /** Assembles the immutable CSR topology. PageRank is left uniform here and
     *  computed by the hub-ranking step after build. */
    public RailTopology build() {
        int n = stopIds.size();

        String[] stopId = stopIds.toArray(new String[0]);
        String[] name = names.toArray(new String[0]);
        double[] lat = lats.stream().mapToDouble(Double::doubleValue).toArray();
        double[] lon = lons.stream().mapToDouble(Double::doubleValue).toArray();
        String[] lineNamesArr = lineNames.toArray(new String[0]);
        int[][] stationLines = new int[n][];
        for (int i = 0; i < n; i++) {
            stationLines[i] = stationLineSets.get(i).stream().mapToInt(Integer::intValue).toArray();
        }
        Map<String, Integer> indexOf = new LinkedHashMap<>(stationIndex);

        // ---- Adjacency CSR --------------------------------------------------
        int[] adjPtr = new int[n + 1];
        for (Long k : edges.keySet()) {
            adjPtr[(int) (k >> 32) + 1]++;
        }
        for (int i = 0; i < n; i++) {
            adjPtr[i + 1] += adjPtr[i];
        }
        int e = edges.size();
        int[] adjTarget = new int[e];
        int[] adjWeight = new int[e];
        int[] adjRunSlack = new int[e];
        int[] cursor = Arrays.copyOf(adjPtr, n);
        for (Map.Entry<Long, int[]> en : edges.entrySet()) {
            int from = (int) (en.getKey() >> 32);
            int to = en.getKey().intValue();
            int pos = cursor[from]++;
            adjTarget[pos] = to;
            adjWeight[pos] = en.getValue()[0];
            adjRunSlack[pos] = en.getValue()[1];
        }
        int[] dwellSlackSec = new int[n];
        dwellSlack.forEach((s, v) -> dwellSlackSec[s] = v);

        // ---- Transfers CSR --------------------------------------------------
        int[] xferPtr = new int[n + 1];
        for (Long k : transfers.keySet()) {
            xferPtr[(int) (k >> 32) + 1]++;
        }
        for (int i = 0; i < n; i++) {
            xferPtr[i + 1] += xferPtr[i];
        }
        int x = transfers.size();
        int[] xferTarget = new int[x];
        int[] xferMin = new int[x];
        int[] xferSlack = new int[x];
        int[] xcursor = Arrays.copyOf(xferPtr, n);
        for (Map.Entry<Long, int[]> en : transfers.entrySet()) {
            int from = (int) (en.getKey() >> 32);
            int to = en.getKey().intValue();
            int pos = xcursor[from]++;
            xferTarget[pos] = to;
            xferMin[pos] = en.getValue()[0];
            xferSlack[pos] = en.getValue()[1];
        }

        // ---- RAPTOR routes --------------------------------------------------
        int r = routes.size();
        int[] routeStopsPtr = new int[r + 1];
        int[] tripTimesBase = new int[r];
        int[] routeTripCount = new int[r];
        int[] routeLine = new int[r];
        List<RouteAccum> routeList = new ArrayList<>(routes.values());

        int stopsTotal = 0;
        int timesTotal = 0;
        for (int ri = 0; ri < r; ri++) {
            RouteAccum ra = routeList.get(ri);
            int s = ra.pattern.length;
            routeStopsPtr[ri + 1] = routeStopsPtr[ri] + s;
            routeTripCount[ri] = ra.arr.size();
            tripTimesBase[ri] = timesTotal;
            routeLine[ri] = lineIndex.getOrDefault(ra.line, 0);
            stopsTotal += s;
            timesTotal += s * ra.arr.size();
        }
        int[] routeStops = new int[stopsTotal];
        int[] tripArrSec = new int[timesTotal];
        int[] tripDepSec = new int[timesTotal];

        for (int ri = 0; ri < r; ri++) {
            RouteAccum ra = routeList.get(ri);
            int s = ra.pattern.length;
            System.arraycopy(ra.pattern, 0, routeStops, routeStopsPtr[ri], s);

            // Sort this route's trips by departure at the first stop.
            Integer[] order = new Integer[ra.arr.size()];
            for (int i = 0; i < order.length; i++) order[i] = i;
            Arrays.sort(order, (a, b) -> Integer.compare(ra.dep.get(a)[0], ra.dep.get(b)[0]));

            int base = tripTimesBase[ri];
            for (int t = 0; t < order.length; t++) {
                int[] a = ra.arr.get(order[t]);
                int[] d = ra.dep.get(order[t]);
                int off = base + t * s;
                System.arraycopy(a, 0, tripArrSec, off, s);
                System.arraycopy(d, 0, tripDepSec, off, s);
            }
        }

        // ---- stop -> routes CSR --------------------------------------------
        int[] stopRoutesPtr = new int[n + 1];
        for (int ri = 0; ri < r; ri++) {
            for (int stop : routeList.get(ri).pattern) {
                stopRoutesPtr[stop + 1]++;
            }
        }
        for (int i = 0; i < n; i++) {
            stopRoutesPtr[i + 1] += stopRoutesPtr[i];
        }
        int srTotal = stopRoutesPtr[n];
        int[] stopRoutes = new int[srTotal];
        int[] stopRoutePos = new int[srTotal];
        int[] srCursor = Arrays.copyOf(stopRoutesPtr, n);
        for (int ri = 0; ri < r; ri++) {
            int[] pat = routeList.get(ri).pattern;
            for (int p = 0; p < pat.length; p++) {
                int pos = srCursor[pat[p]]++;
                stopRoutes[pos] = ri;
                stopRoutePos[pos] = p;
            }
        }

        double[] pagerank = new double[n];
        Arrays.fill(pagerank, n == 0 ? 0.0 : 1.0 / n);

        NetworkGeometry geometry = (segmentGeom == null || segmentGeom.isEmpty())
                ? NetworkGeometry.empty()
                : new NetworkGeometry(segmentGeom);

        return new RailTopology(n, stopId, name, lat, lon, lineNamesArr, stationLines, indexOf,
                adjPtr, adjTarget, adjWeight, adjRunSlack, dwellSlackSec,
                xferPtr, xferTarget, xferMin, xferSlack,
                r, routeStopsPtr, routeStops, tripTimesBase, routeTripCount,
                tripArrSec, tripDepSec, stopRoutesPtr, stopRoutes, stopRoutePos, routeLine,
                geometry, pagerank, graphVersion);
    }
}
