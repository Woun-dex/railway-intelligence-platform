package com.rail.platform.graph.infrastructure.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.application.HubRankingService;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

/**
 * Loads the network on startup: read GTFS → compute the hub vector → publish the
 * snapshot to Hazelcast (and cache it in heap). Runs before the Kafka consumer
 * starts so the engines never see a null topology.
 *
 * <p>If a snapshot already exists in the cluster (this member re-joined an
 * existing one), the load is skipped — recovery is automatic.
 */
@Component
@Order(0)
public class TopologyBootstrapRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TopologyBootstrapRunner.class);

    private final GtfsTopologyLoader loader;
    private final HubRankingService hubRanking;
    private final TopologyRepository repository;

    public TopologyBootstrapRunner(GtfsTopologyLoader loader, HubRankingService hubRanking,
                                   TopologyRepository repository) {
        this.loader = loader;
        this.hubRanking = hubRanking;
        this.repository = repository;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (repository.current() != null) {
            log.info("Topology already present (version={}), skipping load", repository.version());
            return;
        }
        long t0 = System.nanoTime();
        RailTopology topology = loader.load();
        topology = topology.withPagerank(hubRanking.pageRank(topology));
        repository.publish(topology);
        log.info("Topology bootstrapped in {} ms (version={}, stations={}, edges={}, routes={})",
                (System.nanoTime() - t0) / 1_000_000,
                topology.graphVersion(), topology.stationCount(),
                topology.edgeCount(), topology.routeCount());
    }
}
