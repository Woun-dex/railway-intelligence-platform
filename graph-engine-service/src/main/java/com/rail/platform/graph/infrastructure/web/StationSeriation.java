package com.rail.platform.graph.infrastructure.web;

import org.springframework.stereotype.Component;

import com.rail.platform.graph.domain.model.RailTopology;

/**
 * Produces a display ordering of stations that makes the adjacency matrix
 * legible. Stations are walked in route order (line by line), so consecutive
 * stops on a line sit next to each other on both axes — the matrix then shows
 * near-diagonal blocks per line and clear off-diagonal marks at interchanges,
 * instead of a random scatter.
 */
@Component
public class StationSeriation {

    /** @return {@code order[displayPosition] = stationIndex}. */
    public int[] order(RailTopology t) {
        int n = t.stationCount();
        boolean[] placed = new boolean[n];
        int[] order = new int[n];
        int c = 0;
        for (int r = 0; r < t.routeCount(); r++) {
            int stops = t.numStops(r);
            for (int p = 0; p < stops; p++) {
                int s = t.routeStop(r, p);
                if (!placed[s]) {
                    placed[s] = true;
                    order[c++] = s;
                }
            }
        }
        for (int s = 0; s < n; s++) {
            if (!placed[s]) {
                order[c++] = s;
            }
        }
        return order;
    }
}
