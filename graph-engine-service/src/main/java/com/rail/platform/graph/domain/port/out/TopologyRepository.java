package com.rail.platform.graph.domain.port.out;

import com.rail.platform.graph.domain.model.RailTopology;

/**
 * Outbound port for the distributed topology store (Hazelcast adapter).
 *
 * <p>The store provides distribution, recovery and version numbering. The engines
 * read the hot-path arrays from the in-heap object returned by {@link #current()},
 * not from the grid, so a grid round-trip never sits on the critical path.
 */
public interface TopologyRepository {

    /** Publishes a new topology snapshot and bumps the graph version. */
    void publish(RailTopology topology);

    /** The current in-heap topology, or {@code null} if none has been loaded. */
    RailTopology current();

    /** The current graph version, or {@code null} if none has been loaded. */
    String version();
}
