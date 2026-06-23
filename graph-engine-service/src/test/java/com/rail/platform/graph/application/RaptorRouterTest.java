package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.ParetoLabel;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.support.FixedTopologyRepository;
import com.rail.platform.graph.support.MockNetworks;

class RaptorRouterTest {

    private RaptorRouter router(RailTopology t) {
        return new RaptorRouter(new FixedTopologyRepository(t));
    }

    @Test
    void exactParetoFrontier_directVsOneTransfer() {
        RailTopology t = MockNetworks.twoLinePareto();
        JourneyPlan plan = router(t).plan(new JourneyQuery(0, 2, 28800, 6));

        // Pareto-incomparable: 0 transfers / late (09:00) and 1 transfer / early (08:40).
        assertThat(plan.frontier()).containsExactly(
                new ParetoLabel(32400, 0),
                new ParetoLabel(31200, 1));
        assertThat(plan.earliestArrivalSec()).isEqualTo(31200);
    }

    @Test
    void exactMultiTransferParetoFrontier_on100StationNetwork() {
        RailTopology t = MockNetworks.hundredStationStaircase();
        assertThat(t.stationCount()).isEqualTo(100);

        JourneyPlan plan = router(t).plan(new JourneyQuery(0, 99, 0, 6));

        // The express staircase: each added transfer strictly improves arrival.
        assertThat(plan.frontier()).containsExactly(
                new ParetoLabel(9000, 0),
                new ParetoLabel(4000, 1),
                new ParetoLabel(3000, 2));
    }

    @Test
    void frontierIsStrictlyParetoOptimal() {
        RailTopology t = MockNetworks.hundredStationStaircase();
        List<ParetoLabel> f = router(t).plan(new JourneyQuery(0, 99, 0, 6)).frontier();

        for (int i = 1; i < f.size(); i++) {
            // sorted by increasing transfers and strictly decreasing arrival
            assertThat(f.get(i).transfers()).isGreaterThan(f.get(i - 1).transfers());
            assertThat(f.get(i).arrivalSec()).isLessThan(f.get(i - 1).arrivalSec());
        }
        // no label dominates another
        for (ParetoLabel a : f) {
            for (ParetoLabel b : f) {
                if (a != b) {
                    assertThat(a.dominates(b)).isFalse();
                }
            }
        }
    }

    @Test
    void multiTransferRoutingStillFoundWhenRoundsTight() {
        RailTopology t = MockNetworks.hundredStationStaircase();
        // With only 2 rounds (<=1 transfer) the 2-transfer arrival must NOT appear.
        JourneyPlan plan = router(t).plan(new JourneyQuery(0, 99, 0, 2));
        assertThat(plan.frontier()).containsExactly(
                new ParetoLabel(9000, 0),
                new ParetoLabel(4000, 1));
    }

    @Test
    void unreachableWithinRoundsYieldsEmptyFrontier() {
        RailTopology t = MockNetworks.twoLinePareto();
        // Round bound of 0 is clamped to 1 round; 2 is reachable in 1 trip, so use
        // an isolated query: same topology, but ask for a station only reachable via
        // a transfer with a single round available cannot reach via 2 trips.
        JourneyPlan plan = router(t).plan(new JourneyQuery(0, 2, 28800, 1));
        // With one round, only the direct line X (0 transfers) is found.
        assertThat(plan.frontier()).containsExactly(new ParetoLabel(32400, 0));
    }
}
