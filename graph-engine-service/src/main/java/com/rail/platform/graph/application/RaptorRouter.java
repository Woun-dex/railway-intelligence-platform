package com.rail.platform.graph.application;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.JourneyPath;
import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.NetworkGeometry;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.ParetoLabel;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.in.PlanJourneyUseCase;
import com.rail.platform.graph.domain.port.out.TopologyRepository;


@Service
public class RaptorRouter implements PlanJourneyUseCase {

    private final TopologyRepository repository;
    private final ThreadLocal<RaptorScratch> scratch = ThreadLocal.withInitial(RaptorScratch::new);

    public RaptorRouter(TopologyRepository repository) {
        this.repository = repository;
    }

    @Override
    public JourneyPlan plan(JourneyQuery q) {
        RailTopology t = repository.current();
        if (t == null || !t.isStation(q.originStation()) || !t.isStation(q.destStation())) {
            return new JourneyPlan(q.originStation(), q.destStation(), List.of());
        }
        return plan(t, q);
    }

    @Override
    public JourneyPath planPath(JourneyQuery q) {
        RailTopology t = repository.current();
        if (t == null || !t.isStation(q.originStation()) || !t.isStation(q.destStation())) {
            return JourneyPath.unreachable(q.originStation(), q.destStation(), q.departureSec());
        }
        return planPath(t, q);
    }

    /** Direct entry point used by tests/benchmarks that hold a topology in hand. */
    public JourneyPlan plan(RailTopology t, JourneyQuery q) {
        RaptorScratch s = scratch.get();
        List<ParetoLabel> frontier = runScan(t, q, s);
        return new JourneyPlan(q.originStation(), q.destStation(), frontier);
    }

    /** Run the scan, then reconstruct the earliest-arrival journey from predecessors. */
    public JourneyPath planPath(RailTopology t, JourneyQuery q) {
        RaptorScratch s = scratch.get();
        runScan(t, q, s);
        return reconstruct(t, q, s);
    }

    // -----------------------------------------------------------------------
    // Core scan (shared) — fills scratch arrivals + predecessors, returns frontier
    // -----------------------------------------------------------------------
    private List<ParetoLabel> runScan(RailTopology t, JourneyQuery q, RaptorScratch s) {
        int n = t.stationCount();
        int origin = q.originStation();
        int dest = q.destStation();

        s.ensure(n, t.routeCount());
        s.reset(n);

        final int INF = RailTopology.INF;
        s.prev[origin] = q.departureSec();
        s.fromStop[origin] = -1;
        s.viaRoute[origin] = -2; // origin sentinel
        s.addCurMarked(origin);

        // Initial foot transfers from the origin.
        for (int x = t.xferBegin(origin); x < t.xferEnd(origin); x++) {
            int target = t.xferTarget(x);
            int arr = q.departureSec() + t.xferMinSec(x);
            if (arr < s.prev[target]) {
                s.prev[target] = arr;
                s.fromStop[target] = origin;
                s.viaRoute[target] = -1; // walk
                s.addCurMarked(target);
            }
        }

        List<ParetoLabel> frontier = new ArrayList<>();
        int lastDestArr = INF;

        int rounds = Math.max(1, q.maxRounds());
        for (int k = 1; k <= rounds; k++) {
            System.arraycopy(s.prev, 0, s.cur, 0, n);

            // 1. Accumulate routes serving the marked stops, at their earliest position.
            s.queueReset();
            for (int i = 0; i < s.curMarkedCount; i++) {
                int stop = s.curMarked[i];
                for (int sr = t.stopRoutesBegin(stop); sr < t.stopRoutesEnd(stop); sr++) {
                    int route = t.stopRoute(sr);
                    int pos = t.stopRoutePos(sr);
                    if (pos < s.queuePos[route]) {
                        if (s.queuePos[route] == INF) {
                            s.queueList[s.queueCount++] = route;
                        }
                        s.queuePos[route] = pos;
                    }
                }
            }
            s.nextReset();

            // 2. Traverse each route once, riding the earliest catchable trip.
            for (int qi = 0; qi < s.queueCount; qi++) {
                int route = s.queueList[qi];
                int stops = t.numStops(route);
                int trip = -1;
                int boardPos = -1;
                int boardStation = -1;
                for (int pos = s.queuePos[route]; pos < stops; pos++) {
                    int stop = t.routeStop(route, pos);
                    if (trip != -1) {
                        int arr = t.arrSec(route, trip, pos);
                        if (arr < s.cur[stop] && arr < s.cur[dest]) {
                            s.cur[stop] = arr;
                            s.fromStop[stop] = boardStation;
                            s.viaRoute[stop] = route;
                            s.viaBoardPos[stop] = boardPos;
                            s.viaPos[stop] = pos;
                            s.viaTrip[stop] = trip;
                            s.addRouteImproved(stop);
                            s.addNext(stop);
                        }
                    }
                    int ps = s.prev[stop];
                    if (ps != INF && (trip == -1 || ps <= t.depSec(route, trip, pos))) {
                        int nt = earliestTrip(t, route, pos, ps);
                        if (nt != -1) {
                            trip = nt;
                            boardPos = pos;
                            boardStation = stop;
                        }
                    }
                }
            }

            // 3. Relax foot transfers out of the stops improved by riding.
            for (int i = 0; i < s.routeImprovedCount; i++) {
                int p = s.routeImproved[i];
                int ap = s.cur[p];
                for (int x = t.xferBegin(p); x < t.xferEnd(p); x++) {
                    int target = t.xferTarget(x);
                    int arr = ap + t.xferMinSec(x);
                    if (arr < s.cur[target]) {
                        s.cur[target] = arr;
                        s.fromStop[target] = p;
                        s.viaRoute[target] = -1; // walk
                        s.addNext(target);
                    }
                }
            }

            // 4. Pareto frontier point: only if this round beat the best arrival so far.
            if (s.cur[dest] < lastDestArr) {
                frontier.add(new ParetoLabel(s.cur[dest], k - 1));
                lastDestArr = s.cur[dest];
            }
            if (s.nextMarkedCount == 0) {
                break;
            }

            // 5. Roll forward.
            System.arraycopy(s.cur, 0, s.prev, 0, n);
            s.promoteNextToCur();
        }
        return frontier;
    }

    /** Trace predecessors from the destination back to the origin into ordered legs. */
    private JourneyPath reconstruct(RailTopology t, JourneyQuery q, RaptorScratch s) {
        int origin = q.originStation();
        int dest = q.destStation();
        if (s.cur[dest] >= RailTopology.INF) {
            return JourneyPath.unreachable(origin, dest, q.departureSec());
        }

        List<JourneyPath.Leg> legs = new ArrayList<>();
        int c = dest;
        int rides = 0;
        int guard = 0;
        while (c != origin && guard++ < 8192) {
            int vr = s.viaRoute[c];
            if (vr == -2) {
                break; // reached the origin sentinel
            }
            if (vr == -1) {
                int p = s.fromStop[c];
                if (p < 0) {
                    break;
                }
                legs.add(new JourneyPath.Leg("WALK", new int[] {p, c}, s.cur[p], s.cur[c], null));
                c = p;
            } else {
                int bpos = s.viaBoardPos[c];
                int dpos = s.viaPos[c];
                int trip = s.viaTrip[c];
                int len = Math.max(2, dpos - bpos + 1);
                int[] stops = new int[len];
                for (int kk = 0; kk < len; kk++) {
                    stops[kk] = t.routeStop(vr, bpos + kk);
                }
                legs.add(new JourneyPath.Leg("RIDE", stops,
                        t.depSec(vr, trip, bpos), t.arrSec(vr, trip, dpos), legShape(t, stops)));
                rides++;
                c = s.fromStop[c];
            }
        }
        Collections.reverse(legs);
        int transfers = Math.max(0, rides - 1);
        return new JourneyPath(origin, dest, true, q.departureSec(), s.cur[dest], transfers, legs);
    }

    /**
     * Real track polyline ({@code [[lon, lat], ...]}) for a ride leg: concatenate
     * the {@code shapes.txt}-derived curve of each consecutive station pair,
     * falling back to a straight station-to-station segment where no curve exists.
     * Returns {@code null} when the topology carries no geometry at all.
     */
    private static double[][] legShape(RailTopology t, int[] stops) {
        NetworkGeometry g = t.geometry();
        if (g == null || g.isEmpty()) {
            return null;
        }
        List<double[]> pts = new ArrayList<>();
        for (int i = 0; i < stops.length - 1; i++) {
            float[] seg = g.between(stops[i], stops[i + 1]);
            if (seg != null) {
                for (int p = 0; p < seg.length; p += 2) {
                    addPoint(pts, seg[p], seg[p + 1]);
                }
            } else {
                addPoint(pts, t.lon(stops[i]), t.lat(stops[i]));
                addPoint(pts, t.lon(stops[i + 1]), t.lat(stops[i + 1]));
            }
        }
        return pts.toArray(new double[0][]);
    }

    /** Appends a point unless it duplicates the previous one (segment joints overlap). */
    private static void addPoint(List<double[]> pts, double lon, double lat) {
        if (!pts.isEmpty()) {
            double[] prev = pts.get(pts.size() - 1);
            if (prev[0] == lon && prev[1] == lat) {
                return;
            }
        }
        pts.add(new double[] {lon, lat});
    }

    /** Earliest trip on {@code route} whose departure at {@code pos} is at or after {@code time}. */
    private static int earliestTrip(RailTopology t, int route, int pos, int time) {
        int lo = 0;
        int hi = t.numTrips(route) - 1;
        int ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (t.depSec(route, mid, pos) >= time) {
                ans = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return ans;
    }

    /**
     * Per-thread, reusable scan buffers. Sized to the current topology and reset
     * (not reallocated) between queries to keep the hot path allocation-free.
     */
    static final class RaptorScratch {
        int n = -1;
        int r = -1;
        int[] prev;            // tau_{k-1} arrival per stop
        int[] cur;             // tau_k arrival per stop
        int[] queuePos;        // earliest marked position per route (INF = absent)
        int[] queueList;
        int queueCount;
        int[] curMarked;
        int curMarkedCount;
        int[] nextMarked;
        int nextMarkedCount;
        boolean[] inNext;
        int[] routeImproved;
        boolean[] inRouteImproved;
        int routeImprovedCount;
        // ---- journey reconstruction predecessors (per stop) ----
        int[] fromStop;        // predecessor station for the best arrival
        int[] viaRoute;        // route ridden to reach this stop; -1 = walk, -2 = origin
        int[] viaBoardPos;     // position on viaRoute where the trip was boarded
        int[] viaPos;          // position on viaRoute of this stop
        int[] viaTrip;         // trip index ridden

        void ensure(int stations, int routes) {
            if (stations != n) {
                n = stations;
                prev = new int[n];
                cur = new int[n];
                curMarked = new int[n];
                nextMarked = new int[n];
                inNext = new boolean[n];
                routeImproved = new int[n];
                inRouteImproved = new boolean[n];
                fromStop = new int[n];
                viaRoute = new int[n];
                viaBoardPos = new int[n];
                viaPos = new int[n];
                viaTrip = new int[n];
            }
            if (routes != r) {
                r = routes;
                queuePos = new int[r];
                queueList = new int[r];
                Arrays.fill(queuePos, RailTopology.INF);
            }
        }

        void reset(int stations) {
            Arrays.fill(prev, 0, stations, RailTopology.INF);
            Arrays.fill(cur, 0, stations, RailTopology.INF);
            Arrays.fill(fromStop, 0, stations, -1);
            Arrays.fill(viaRoute, 0, stations, -2);
            for (int i = 0; i < nextMarkedCount; i++) {
                inNext[nextMarked[i]] = false;
            }
            for (int i = 0; i < routeImprovedCount; i++) {
                inRouteImproved[routeImproved[i]] = false;
            }
            for (int i = 0; i < queueCount; i++) {
                queuePos[queueList[i]] = RailTopology.INF;
            }
            curMarkedCount = 0;
            nextMarkedCount = 0;
            routeImprovedCount = 0;
            queueCount = 0;
        }

        void addCurMarked(int stop) {
            curMarked[curMarkedCount++] = stop;
        }

        void queueReset() {
            for (int i = 0; i < queueCount; i++) {
                queuePos[queueList[i]] = RailTopology.INF;
            }
            queueCount = 0;
        }

        void nextReset() {
            for (int i = 0; i < nextMarkedCount; i++) {
                inNext[nextMarked[i]] = false;
            }
            for (int i = 0; i < routeImprovedCount; i++) {
                inRouteImproved[routeImproved[i]] = false;
            }
            nextMarkedCount = 0;
            routeImprovedCount = 0;
        }

        void addNext(int stop) {
            if (!inNext[stop]) {
                inNext[stop] = true;
                nextMarked[nextMarkedCount++] = stop;
            }
        }

        void addRouteImproved(int stop) {
            if (!inRouteImproved[stop]) {
                inRouteImproved[stop] = true;
                routeImproved[routeImprovedCount++] = stop;
            }
        }

        void promoteNextToCur() {
            System.arraycopy(nextMarked, 0, curMarked, 0, nextMarkedCount);
            curMarkedCount = nextMarkedCount;
        }
    }
}
