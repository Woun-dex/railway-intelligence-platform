package com.rail.platform.graph.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

/**
 * Recomputes the hub vector on demand and republishes the topology.
 *
 * <p>The hub ranking is no longer frozen at boot: a cron job refreshes it
 * periodically and a PCC directive triggers it immediately. Each recompute folds
 * the current {@link OperationalState} overrides into the PageRank teleport vector
 * (personalized PageRank), then publishes the updated snapshot so every member and
 * the viewer reflect the new hubs at once. Serialized so concurrent triggers
 * (cron + PCC) never race.
 */
@Service
public class HubRankingCoordinator {

    private static final Logger log = LoggerFactory.getLogger(HubRankingCoordinator.class);

    private final TopologyRepository repository;
    private final HubRankingService hubRanking;
    private final OperationalState operationalState;

    public HubRankingCoordinator(TopologyRepository repository, HubRankingService hubRanking,
                                 OperationalState operationalState) {
        this.repository = repository;
        this.hubRanking = hubRanking;
        this.operationalState = operationalState;
    }

    public synchronized RailTopology recompute(String reason) {
        RailTopology t = repository.current();
        if (t == null) {
            log.warn("hub recompute skipped ({}): no topology loaded", reason);
            return null;
        }
        long t0 = System.nanoTime();
        double[] personalization = operationalState.isEmpty()
                ? null
                : operationalState.personalization(t);
        double[] pr = hubRanking.pageRank(t, personalization);
        RailTopology updated = t.withPagerank(pr);
        repository.publish(updated);
        log.info("Hub ranking recomputed ({}) in {} ms — overrides: {} line(s), {} station(s)",
                reason, (System.nanoTime() - t0) / 1_000_000,
                operationalState.lines().size(), operationalState.stations().size());
        return updated;
    }
}
