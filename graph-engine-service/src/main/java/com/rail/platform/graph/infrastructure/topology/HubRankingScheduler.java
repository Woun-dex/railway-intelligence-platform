package com.rail.platform.graph.infrastructure.topology;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.application.HubRankingCoordinator;

/**
 * Cron-driven hub-ranking refresh. Recomputes the PageRank hub vector on a
 * schedule so it stays consistent with the latest operational overrides — the
 * "cron job runner" half of the dynamic ranking (the PCC command endpoint is the
 * other). Disable by setting {@code rail.graph.pagerank.refresh-cron: -}.
 */
@Component
public class HubRankingScheduler {

    private final HubRankingCoordinator coordinator;

    public HubRankingScheduler(HubRankingCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Scheduled(cron = "${rail.graph.pagerank.refresh-cron:0 */5 * * * *}", zone = "Europe/Paris")
    public void refresh() {
        coordinator.recompute("cron");
    }
}
