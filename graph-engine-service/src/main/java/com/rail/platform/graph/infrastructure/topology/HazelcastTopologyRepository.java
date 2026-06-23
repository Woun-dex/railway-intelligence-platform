package com.rail.platform.graph.infrastructure.topology;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.graph.infrastructure.config.HazelcastConfig;

import jakarta.annotation.PostConstruct;

/**
 * Hazelcast-backed {@link TopologyRepository}.
 *
 * <p>The snapshot lives in a backed-up IMap (distribution + recovery). For the
 * hot path we keep a single in-heap reference, refreshed on publish and via a map
 * listener — so propagation and RAPTOR read flat arrays directly, with no grid
 * round-trip or deserialization on the critical path.
 */
@Component
public class HazelcastTopologyRepository implements TopologyRepository {

    private static final Logger log = LoggerFactory.getLogger(HazelcastTopologyRepository.class);

    private final IMap<String, RailTopology> map;
    private volatile RailTopology local;

    public HazelcastTopologyRepository(HazelcastInstance hazelcast) {
        this.map = hazelcast.getMap(HazelcastConfig.TOPOLOGY_MAP);
    }

    @PostConstruct
    void init() {
        // Adopt any snapshot already present in the cluster (e.g. this member
        // restarted and re-joined), and stay in sync on future updates.
        RailTopology existing = map.get(HazelcastConfig.TOPOLOGY_KEY);
        if (existing != null) {
            local = existing;
            log.info("Adopted existing topology snapshot (version={})", existing.graphVersion());
        }
        map.addEntryListener((EntryAddedListener<String, RailTopology>) e -> refresh(e.getValue()), true);
        map.addEntryListener((EntryUpdatedListener<String, RailTopology>) e -> refresh(e.getValue()), true);
    }

    private void refresh(RailTopology t) {
        if (t != null) {
            local = t;
            log.info("Refreshed in-heap topology from grid (version={})", t.graphVersion());
        }
    }

    @Override
    public void publish(RailTopology topology) {
        local = topology;
        map.put(HazelcastConfig.TOPOLOGY_KEY, topology);
    }

    @Override
    public RailTopology current() {
        RailTopology t = local;
        if (t == null) {
            t = map.get(HazelcastConfig.TOPOLOGY_KEY);
            if (t != null) {
                local = t;
            }
        }
        return t;
    }

    @Override
    public String version() {
        RailTopology t = current();
        return t == null ? null : t.graphVersion();
    }
}
