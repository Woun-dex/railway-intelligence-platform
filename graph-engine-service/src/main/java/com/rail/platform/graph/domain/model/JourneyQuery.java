package com.rail.platform.graph.domain.model;

/**
 * A RAPTOR query: leave {@code originStation} at {@code departureSec}
 * (second-of-day) and reach {@code destStation}, exploring up to
 * {@code maxRounds} rounds (i.e. up to {@code maxRounds - 1} transfers).
 */
public record JourneyQuery(int originStation, int destStation, int departureSec, int maxRounds) {

    public static JourneyQuery of(int origin, int dest, int departureSec) {
        return new JourneyQuery(origin, dest, departureSec, 6);
    }
}
