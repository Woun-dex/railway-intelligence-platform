package com.rail.platform.graph.domain.model;

/**
 * A point on the multi-criteria Pareto frontier produced by RAPTOR: the earliest
 * {@code arrivalSec} (second-of-day) reachable with exactly {@code transfers}
 * transfers.
 */
public record ParetoLabel(int arrivalSec, int transfers) {

    /**
     * True if this label weakly dominates {@code other} (no worse on either axis)
     * and strictly better on at least one. A label that is dominated by any other
     * is not on the frontier.
     */
    public boolean dominates(ParetoLabel other) {
        boolean noWorse = arrivalSec <= other.arrivalSec && transfers <= other.transfers;
        boolean strictlyBetter = arrivalSec < other.arrivalSec || transfers < other.transfers;
        return noWorse && strictlyBetter;
    }
}
