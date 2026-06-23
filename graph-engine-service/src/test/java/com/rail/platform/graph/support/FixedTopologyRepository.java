package com.rail.platform.graph.support;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

/** A trivial in-heap {@link TopologyRepository} for engine tests. */
public final class FixedTopologyRepository implements TopologyRepository {

    private RailTopology topology;

    public FixedTopologyRepository(RailTopology topology) {
        this.topology = topology;
    }

    @Override
    public void publish(RailTopology t) {
        this.topology = t;
    }

    @Override
    public RailTopology current() {
        return topology;
    }

    @Override
    public String version() {
        return topology == null ? null : topology.graphVersion();
    }
}
