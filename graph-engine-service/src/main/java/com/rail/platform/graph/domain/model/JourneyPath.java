package com.rail.platform.graph.domain.model;

import java.util.List;

/**
 * A reconstructed RAPTOR journey: the concrete leg-by-leg path for one
 * origin-destination query (the earliest-arrival journey), suitable for drawing
 * on a map. Distinct from {@link JourneyPlan}, which carries only the
 * {@code (arrival, transfers)} Pareto frontier without the path itself.
 *
 * <p>Each {@link Leg} lists the station indices it passes through in order; a
 * {@code RIDE} leg is one boarded trip (its {@code stops} are the consecutive
 * stations on that route from board to alight), a {@code WALK} leg is a foot
 * transfer between two stations.
 *
 * <p>{@link Leg#shape} carries the real track polyline ({@code [[lon, lat], ...]})
 * from GTFS {@code shapes.txt} so the viewer follows true curvature; it is
 * {@code null} for foot transfers and when the feed has no geometry, in which
 * case the viewer falls back to straight station-to-station segments.
 */
public record JourneyPath(int originStation, int destStation, boolean reachable,
                          int departureSec, int arrivalSec, int transfers, List<Leg> legs) {

    public record Leg(String kind, int[] stops, int depSec, int arrSec, double[][] shape) {
    }

    public static JourneyPath unreachable(int origin, int dest, int departureSec) {
        return new JourneyPath(origin, dest, false, departureSec, -1, 0, List.of());
    }
}
