package com.rail.platform.graph.application;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.ParetoLabel;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.in.PlanJourneyUseCase;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

/**
 * Engine 1b — a localized RAPTOR (Round-bAsed Public Transit Optimized Router).
 *
 * <p>Each round relaxes one additional trip, scanning the flat timetable arrays
 * sequentially (no priority queue, no pointer chasing) to keep the CPU caches
 * hot. Because round {@code k} corresponds to {@code k − 1} transfers, the
 * Pareto frontier over {@code (arrival_time, transfers)} falls out for free: a
 * round contributes a frontier point only when it strictly improves the arrival
 * at the destination.
 *
 * <p><b>State isolation:</b> all mutable scan state lives in a per-thread
 * {@link RaptorScratch}; the shared {@link RailTopology} is read-only. A query
 * therefore runs entirely within its own thread boundary with no contention.
 */
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

    /** Direct entry point used by tests/benchmarks that hold a topology in hand. */
    public JourneyPlan plan(RailTopology t, JourneyQuery q) {
        int n = t.stationCount();
        int origin = q.originStation();
        int dest = q.destStation();

        RaptorScratch s = scratch.get();
        s.ensure(n, t.routeCount());
        s.reset(n);

        final int INF = RailTopology.INF;
        s.prev[origin] = q.departureSec();
        s.addCurMarked(origin);
        // Initial foot transfers from the origin.
        for (int x = t.xferBegin(origin); x < t.xferEnd(origin); x++) {
            int target = t.xferTarget(x);
            int arr = q.departureSec() + t.xferMinSec(x);
            if (arr < s.prev[target]) {
                s.prev[target] = arr;
                s.addCurMarked(target);
            }
        }

        List<ParetoLabel> frontier = new ArrayList<>();
        int lastDestArr = INF;

        int rounds = Math.max(1, q.maxRounds());
        for (int k = 1; k <= rounds; k++) {
            System.arraycopy(s.prev, 0, s.cur, 0, n);

            // 1. Accumulate the routes serving the marked stops, at their earliest position.
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
                for (int pos = s.queuePos[route]; pos < stops; pos++) {
                    int stop = t.routeStop(route, pos);
                    if (trip != -1) {
                        int arr = t.arrSec(route, trip, pos);
                        if (arr < s.cur[stop] && arr < s.cur[dest]) {
                            s.cur[stop] = arr;
                            s.addRouteImproved(stop);
                            s.addNext(stop);
                        }
                    }
                    int ps = s.prev[stop];
                    if (ps != INF && (trip == -1 || ps <= t.depSec(route, trip, pos))) {
                        int nt = earliestTrip(t, route, pos, ps);
                        if (nt != -1) {
                            trip = nt;
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

            // 5. Roll forward: prev := tau_k; the next round starts from this round's marks.
            System.arraycopy(s.cur, 0, s.prev, 0, n);
            s.promoteNextToCur();
        }
        return new JourneyPlan(origin, dest, frontier);
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
        int[] queueList;       // touched routes this round
        int queueCount;
        int[] curMarked;       // queue-source stops for this round
        int curMarkedCount;
        int[] nextMarked;      // stops marked this round (queue source for the next)
        int nextMarkedCount;
        boolean[] inNext;      // membership flag for nextMarked
        int[] routeImproved;   // distinct stops improved by riding (transfer origins)
        boolean[] inRouteImproved;
        int routeImprovedCount;

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
            // Clear membership/markers left over from a previous query on this
            // thread, using the leftover counts BEFORE zeroing them — otherwise a
            // reused scratch leaks stale "already marked" flags between queries.
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
