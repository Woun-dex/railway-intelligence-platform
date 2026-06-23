package com.rail.platform.graph.domain.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable, memory-localized representation of the rail network.
 *
 * <p>This is the single hot-path data structure for both engines. It uses
 * <strong>flat, un-boxed primitive arrays</strong> in
 * <a href="https://en.wikipedia.org/wiki/Sparse_matrix#Compressed_sparse_row_(CSR,_CRS_or_Yale_format)">
 * Compressed-Sparse-Row (CSR)</a> form so the propagation and RAPTOR scans walk
 * contiguous memory (maximizing CPU L1/L2 cache hit-rates) with no pointer
 * chasing and no per-node object headers.
 *
 * <p>It implements {@link Serializable} for one purpose only: distribution and
 * recovery as a Hazelcast snapshot. It is <em>never</em> deserialized on the hot
 * path — the engines hold a single in-heap reference and read the arrays
 * directly.
 *
 * <h2>Layouts</h2>
 * <ul>
 *   <li><b>Adjacency</b> (directed station graph, for propagation / PageRank /
 *       the matrix view): {@code adjPtr[i]..adjPtr[i+1]} indexes the out-edges of
 *       station {@code i} in {@code adjTarget} / {@code adjWeightSec} /
 *       {@code adjRunSlackSec}.</li>
 *   <li><b>Transfers</b> (foot connections, CSR): {@code xferPtr[i]..xferPtr[i+1]}
 *       indexes {@code xferTarget} / {@code xferMinSec} / {@code xferSlackSec}.</li>
 *   <li><b>RAPTOR routes</b>: a route is a set of trips sharing one ordered stop
 *       pattern. {@code routeStopsPtr} indexes the pattern in {@code routeStops};
 *       trip times for route {@code r}, trip {@code t}, position {@code p} are at
 *       {@code tripTimesBase[r] + t * numStops(r) + p} in {@code tripArrSec} /
 *       {@code tripDepSec} (trips sorted by departure).
 *       {@code stopRoutesPtr} indexes {@code stopRoutes} / {@code stopRoutePos}
 *       (which routes serve a stop, and at which position).</li>
 * </ul>
 */
public final class RailTopology implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sentinel for "unreachable" / "no time" in second-of-day arithmetic. */
    public static final int INF = Integer.MAX_VALUE / 4;

    // ---- Stations ----------------------------------------------------------
    private final int stationCount;
    private final String[] stopId;
    private final String[] stationName;
    private final double[] lat;
    private final double[] lon;
    private final String[] lineNames;       // line-index space
    private final int[][] stationLines;     // per station: line indices serving it
    private final Map<String, Integer> indexOf;

    // ---- Adjacency (directed station graph, CSR) ---------------------------
    private final int[] adjPtr;             // length stationCount + 1
    private final int[] adjTarget;          // length E
    private final int[] adjWeightSec;       // length E  (min observed run time)
    private final int[] adjRunSlackSec;     // length E  (running recovery margin)
    private final int[] dwellSlackSec;      // length stationCount (dwell recovery)

    // ---- Transfers (foot connections, CSR) --------------------------------
    private final int[] xferPtr;            // length stationCount + 1
    private final int[] xferTarget;         // length X
    private final int[] xferMinSec;         // length X  (minimum transfer time)
    private final int[] xferSlackSec;       // length X  (connection slack)

    // ---- RAPTOR route model -----------------------------------------------
    private final int routeCount;
    private final int[] routeStopsPtr;      // length routeCount + 1
    private final int[] routeStops;         // station indices, per route, in order
    private final int[] tripTimesBase;      // length routeCount (offset into trip*Sec)
    private final int[] routeTripCount;     // length routeCount
    private final int[] tripArrSec;         // flattened arrivals
    private final int[] tripDepSec;         // flattened departures
    private final int[] stopRoutesPtr;      // length stationCount + 1
    private final int[] stopRoutes;         // route indices serving each stop
    private final int[] stopRoutePos;       // position of the stop within that route

    // ---- Hub vector + meta -------------------------------------------------
    private final double[] pagerank;        // length stationCount
    private final String graphVersion;

    /** Package-private: instances are produced by {@link RailTopologyBuilder}. */
    RailTopology(int stationCount, String[] stopId, String[] stationName,
                 double[] lat, double[] lon, String[] lineNames, int[][] stationLines,
                 Map<String, Integer> indexOf,
                 int[] adjPtr, int[] adjTarget, int[] adjWeightSec, int[] adjRunSlackSec,
                 int[] dwellSlackSec,
                 int[] xferPtr, int[] xferTarget, int[] xferMinSec, int[] xferSlackSec,
                 int routeCount, int[] routeStopsPtr, int[] routeStops,
                 int[] tripTimesBase, int[] routeTripCount,
                 int[] tripArrSec, int[] tripDepSec,
                 int[] stopRoutesPtr, int[] stopRoutes, int[] stopRoutePos,
                 double[] pagerank, String graphVersion) {
        this.stationCount = stationCount;
        this.stopId = stopId;
        this.stationName = stationName;
        this.lat = lat;
        this.lon = lon;
        this.lineNames = lineNames;
        this.stationLines = stationLines;
        this.indexOf = indexOf;
        this.adjPtr = adjPtr;
        this.adjTarget = adjTarget;
        this.adjWeightSec = adjWeightSec;
        this.adjRunSlackSec = adjRunSlackSec;
        this.dwellSlackSec = dwellSlackSec;
        this.xferPtr = xferPtr;
        this.xferTarget = xferTarget;
        this.xferMinSec = xferMinSec;
        this.xferSlackSec = xferSlackSec;
        this.routeCount = routeCount;
        this.routeStopsPtr = routeStopsPtr;
        this.routeStops = routeStops;
        this.tripTimesBase = tripTimesBase;
        this.routeTripCount = routeTripCount;
        this.tripArrSec = tripArrSec;
        this.tripDepSec = tripDepSec;
        this.stopRoutesPtr = stopRoutesPtr;
        this.stopRoutes = stopRoutes;
        this.stopRoutePos = stopRoutePos;
        this.pagerank = pagerank;
        this.graphVersion = graphVersion;
    }

    // ---- Stations ----------------------------------------------------------

    public int stationCount() { return stationCount; }

    public String stopId(int i) { return stopId[i]; }

    public String stationName(int i) { return stationName[i]; }

    public double lat(int i) { return lat[i]; }

    public double lon(int i) { return lon[i]; }

    public String[] lineNames() { return lineNames; }

    public int[] linesOf(int i) { return stationLines[i]; }

    /** Internal station index for a GTFS stop id, or {@code -1} if unknown. */
    public int index(String stopId) {
        Integer i = indexOf.get(stopId);
        return i == null ? -1 : i;
    }

    public boolean isStation(int i) { return i >= 0 && i < stationCount; }

    // ---- Adjacency ---------------------------------------------------------

    public int adjBegin(int station) { return adjPtr[station]; }

    public int adjEnd(int station) { return adjPtr[station + 1]; }

    public int adjTarget(int e) { return adjTarget[e]; }

    public int adjWeightSec(int e) { return adjWeightSec[e]; }

    public int adjRunSlackSec(int e) { return adjRunSlackSec[e]; }

    public int dwellSlackSec(int station) { return dwellSlackSec[station]; }

    public int edgeCount() { return adjTarget.length; }

    /** Raw CSR arrays for bulk consumers (PageRank, the matrix endpoint). */
    public int[] adjPtr() { return adjPtr; }

    public int[] adjTargets() { return adjTarget; }

    public int[] adjWeights() { return adjWeightSec; }

    // ---- Transfers ---------------------------------------------------------

    public int xferBegin(int station) { return xferPtr[station]; }

    public int xferEnd(int station) { return xferPtr[station + 1]; }

    public int xferTarget(int x) { return xferTarget[x]; }

    public int xferMinSec(int x) { return xferMinSec[x]; }

    public int xferSlackSec(int x) { return xferSlackSec[x]; }

    // ---- RAPTOR route model ------------------------------------------------

    public int routeCount() { return routeCount; }

    public int numStops(int route) { return routeStopsPtr[route + 1] - routeStopsPtr[route]; }

    public int routeStop(int route, int pos) { return routeStops[routeStopsPtr[route] + pos]; }

    public int numTrips(int route) { return routeTripCount[route]; }

    public int arrSec(int route, int trip, int pos) {
        return tripArrSec[tripTimesBase[route] + trip * numStops(route) + pos];
    }

    public int depSec(int route, int trip, int pos) {
        return tripDepSec[tripTimesBase[route] + trip * numStops(route) + pos];
    }

    public int stopRoutesBegin(int station) { return stopRoutesPtr[station]; }

    public int stopRoutesEnd(int station) { return stopRoutesPtr[station + 1]; }

    public int stopRoute(int sr) { return stopRoutes[sr]; }

    public int stopRoutePos(int sr) { return stopRoutePos[sr]; }

    // ---- Hub vector + meta -------------------------------------------------

    public double pagerank(int station) { return pagerank[station]; }

    public double[] pagerank() { return pagerank; }

    public String graphVersion() { return graphVersion; }

    /**
     * Returns a copy of this topology with a freshly computed hub vector. Cheap:
     * all primitive arrays are shared by reference; only the {@code pagerank}
     * vector and version string are swapped.
     */
    public RailTopology withPagerank(double[] newPagerank) {
        return new RailTopology(stationCount, stopId, stationName, lat, lon, lineNames,
                stationLines, indexOf, adjPtr, adjTarget, adjWeightSec, adjRunSlackSec,
                dwellSlackSec, xferPtr, xferTarget, xferMinSec, xferSlackSec,
                routeCount, routeStopsPtr, routeStops, tripTimesBase, routeTripCount,
                tripArrSec, tripDepSec, stopRoutesPtr, stopRoutes, stopRoutePos,
                newPagerank, graphVersion);
    }
}
