package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.DefaultResourceLoader;

import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.ParetoLabel;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.infrastructure.topology.GtfsTopologyLoader;
import com.rail.platform.graph.infrastructure.topology.SlackModel;
import com.rail.platform.graph.support.FixedTopologyRepository;

/**
 * RAPTOR routing against the real Transilien sample — exercises foot transfers
 * (which the synthetic mock networks don't have) and verifies that a reused
 * router never leaks scan state between queries.
 *
 * <p>Station indices (deterministic from the sample): 0 = Saint-Germain (RER A),
 * 2 = Nanterre-Ville (RER A), 9 = Vincennes (RER A), 15 = Noisy-le-Sec (RER E),
 * 22 = Nanterre-Université (Transilien L). The sample defines a foot transfer
 * Nanterre-Ville ↔ Nanterre-Université (600 s).
 */
class RaptorGtfsRoutingTest {

    private RailTopology sample() {
        return new GtfsTopologyLoader(new DefaultResourceLoader(), new SlackModel(600, 180, 120),
                "classpath:gtfs/transilien-sample", "2", 120).load();
    }

    private RaptorRouter router(RailTopology t) {
        return new RaptorRouter(new FixedTopologyRepository(t));
    }

    @Test
    void sameLineJourneyHasZeroTransfers() {
        RailTopology t = sample();
        JourneyPlan p = router(t).plan(new JourneyQuery(0, 9, 25200, 6)); // SGL -> VINC on RER A
        assertThat(p.frontier()).containsExactly(new ParetoLabel(27540, 0)); // 07:39
    }

    @Test
    void footpathGivesAZeroTransferJourney() {
        RailTopology t = sample();
        // SGL -> Nanterre-Université: ride RER A to Nanterre-Ville (arr 07:13) then
        // walk 600 s to Nanterre-Université (arr 07:23). One train => 0 transfers,
        // and it dominates the 07:29 interchange at La Défense.
        JourneyPlan p = router(t).plan(new JourneyQuery(0, 22, 25200, 6));
        assertThat(p.frontier()).containsExactly(new ParetoLabel(26580, 0));
    }

    @Test
    void transferRequiredWhenNoFootpathShortcut() {
        RailTopology t = sample();
        // SGL (RER A) -> Noisy-le-Sec (RER E): no footpath shortcut, so a change of
        // train (at La Défense) is unavoidable => every option has >= 1 transfer.
        JourneyPlan p = router(t).plan(new JourneyQuery(0, 15, 25200, 6));
        assertThat(p.reachable()).isTrue();
        assertThat(p.frontier()).allSatisfy(label -> assertThat(label.transfers()).isGreaterThanOrEqualTo(1));
    }

    @Test
    void reusedRouterMatchesFreshRouterPerQuery() {
        RailTopology t = sample();
        RaptorRouter shared = router(t);

        int[][] pairs = {{16, 25}, {3, 22}, {0, 15}, {0, 22}, {0, 9}, {2, 22}, {0, 25}};
        for (int[] od : pairs) {
            JourneyQuery q = new JourneyQuery(od[0], od[1], 25200, 6);
            // A throwaway warm-up query on the shared router beforehand, then compare
            // the shared router's answer to a brand-new router's answer.
            shared.plan(new JourneyQuery(11, 15, 26000, 6));
            JourneyPlan onShared = shared.plan(q);
            JourneyPlan onFresh = router(t).plan(q);
            assertThat(onShared.frontier())
                    .as("reused router must match fresh for %d->%d", od[0], od[1])
                    .isEqualTo(onFresh.frontier());
        }
    }
}
