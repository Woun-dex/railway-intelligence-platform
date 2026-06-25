package com.rail.platform.graph.domain.model;

import java.util.List;

/**
 * A station's live departure board at one instant: the next departures grouped
 * <em>per commercial line</em> ("canal"), each with its terminus and a
 * seconds-of-day departure time. This is the source the engine turns into
 * {@code rail.display} events — one message per departure, keyed by line.
 */
public record StationBoard(int station, String stationName, int queryTimeSec, List<LineDepartures> lines) {

    public record LineDepartures(String line, List<Departure> departures) {
    }

    public record Departure(int depSec, int etaSec, String destination) {
    }
}
