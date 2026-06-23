package com.rail.platform.graph.support;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.RailTopologyBuilder;

/**
 * Deterministic in-memory networks for the engine tests. These are fixtures, not
 * the production topology (which is loaded from real GTFS); they let us assert
 * exact routing and propagation outcomes.
 */
public final class MockNetworks {

    private MockNetworks() {
    }

    /**
     * Minimal network with a Pareto-incomparable pair 0&nbsp;→&nbsp;2:
     * <ul>
     *   <li>line X rides 0→2 directly, arriving late (09:00);</li>
     *   <li>line Y (0→1) + line Z (1→2) arrives earlier (08:40) at the cost of one transfer.</li>
     * </ul>
     * Expected frontier: {@code [(32400,0), (31200,1)]}.
     */
    public static RailTopology twoLinePareto() {
        RailTopologyBuilder b = new RailTopologyBuilder();
        for (int i = 0; i < 3; i++) {
            b.station("S" + i, "S" + i, i, 0);
        }
        // X: 0 -> 2 direct, depart 08:00 (28800), arrive 09:00 (32400)
        b.trip("X", new int[]{0, 2}, new int[]{28800, 32400}, new int[]{28800, 32400});
        // Y: 0 -> 1, depart 08:00, arrive 08:20 (30000)
        b.trip("Y", new int[]{0, 1}, new int[]{28800, 30000}, new int[]{28800, 30000});
        // Z: 1 -> 2, depart 08:25 (30300), arrive 08:40 (31200)
        b.trip("Z", new int[]{1, 2}, new int[]{30300, 31200}, new int[]{30300, 31200});
        return b.build();
    }

    /**
     * 100-station network engineered so that 0&nbsp;→&nbsp;99 has an exact
     * three-point Pareto frontier:
     * <ul>
     *   <li>0 transfers: DIRECT, arrive 9000;</li>
     *   <li>1 transfer: FEED(0→50)+EXP(50→99), arrive 4000;</li>
     *   <li>2 transfers: A(0→33)+B(33→66)+C(66→99), arrive 3000.</li>
     * </ul>
     * A slow LOCAL line stops at all 100 stations to keep the network connected
     * and the scan realistic, but never beats the express chain.
     * Expected frontier: {@code [(9000,0), (4000,1), (3000,2)]}.
     */
    public static RailTopology hundredStationStaircase() {
        RailTopologyBuilder b = new RailTopologyBuilder();
        for (int i = 0; i < 100; i++) {
            b.station("S" + i, "S" + i, i, 0);
        }
        // Slow local line over every station (connectivity + scan load).
        int[] all = new int[100];
        for (int i = 0; i < 100; i++) {
            all[i] = i;
        }
        addUniformTrip(b, "LOCAL", all, 0, 330, 30);

        // Exact express staircase to station 99.
        twoStop(b, "DIRECT", 0, 99, 0, 9000);
        twoStop(b, "FEED", 0, 50, 0, 2000);
        twoStop(b, "EXP", 50, 99, 2100, 4000);
        twoStop(b, "A", 0, 33, 0, 1000);
        twoStop(b, "B", 33, 66, 1100, 2000);
        twoStop(b, "C", 66, 99, 2100, 3000);
        return b.build();
    }

    /** Chain 0→1→2→3 with a broken transfer 2→4, for propagation slack tests. */
    public static RailTopology slackChain() {
        RailTopologyBuilder b = new RailTopologyBuilder();
        for (int i = 0; i < 5; i++) {
            b.station("S" + i, "S" + i, i, 0);
        }
        // running edges with 60s run slack each
        b.segment(0, 1, 120, 60);
        b.segment(1, 2, 120, 60);
        b.segment(2, 3, 120, 60);
        // 30s dwell slack at each downstream station
        b.dwellSlack(1, 30);
        b.dwellSlack(2, 30);
        b.dwellSlack(3, 30);
        // transfer 2 -> 4 with only 100s of connection slack
        b.transfer(2, 4, 120, 100);
        return b.build();
    }

    private static void twoStop(RailTopologyBuilder b, String line, int from, int to, int dep, int arr) {
        b.trip(line, new int[]{from, to}, new int[]{dep, arr}, new int[]{dep, arr});
    }

    private static void addUniformTrip(RailTopologyBuilder b, String line, int[] stops,
                                       int startDep, int run, int dwell) {
        int n = stops.length;
        int[] arr = new int[n];
        int[] dep = new int[n];
        int t = startDep;
        for (int i = 0; i < n; i++) {
            arr[i] = t;
            dep[i] = (i == 0 || i == n - 1) ? t : t + dwell;
            if (i < n - 1) {
                t = dep[i] + run;
            }
        }
        b.trip(line, stops, arr, dep);
    }
}
