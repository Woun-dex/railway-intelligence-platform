package com.rail.platform.graph.domain.model;

import java.util.List;

/**
 * The result of a RAPTOR query: the Pareto frontier over
 * {@code (arrival_time, transfers)} for the requested origin-destination pair,
 * ordered by increasing transfer count (and therefore decreasing arrival time).
 * An empty frontier means the destination was unreachable within the round bound.
 */
public record JourneyPlan(int originStation, int destStation, List<ParetoLabel> frontier) {

    public boolean reachable() {
        return !frontier.isEmpty();
    }

    /** The earliest arrival on the frontier regardless of transfers, or -1. */
    public int earliestArrivalSec() {
        int best = -1;
        for (ParetoLabel l : frontier) {
            if (best < 0 || l.arrivalSec() < best) {
                best = l.arrivalSec();
            }
        }
        return best;
    }
}
