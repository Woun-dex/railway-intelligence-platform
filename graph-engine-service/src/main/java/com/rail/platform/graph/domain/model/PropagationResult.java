package com.rail.platform.graph.domain.model;

/**
 * One affected node in a delay cascade: the residual delay at
 * {@code stationIndex} after slack absorption along the path from the source,
 * how much slack was absorbed in total, and whether a transfer connection was
 * broken at this node.
 *
 * @param hop distance (in edges) from the source — 0 is the source station
 */
public record PropagationResult(int stationIndex, int propagatedDelaySec,
                                int absorbedSlackSec, boolean missedConnection, int hop) {
}
