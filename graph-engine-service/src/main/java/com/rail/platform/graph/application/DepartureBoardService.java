package com.rail.platform.graph.application;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.StationBoard;

/**
 * Builds a station's departure board from the in-heap timetable: for every route
 * that calls at the station (other than as its terminus), collect the upcoming
 * trips, group them by commercial line, collapse the duplicate departures that a
 * line's overlapping stop-patterns produce, and keep the next few per line.
 *
 * <p>Pure read over {@link RailTopology}; the scheduled publisher turns the
 * result into {@code rail.display} events.
 */
@Service
public class DepartureBoardService {

    private static final int HORIZON_SEC = 6 * 3600;

    public StationBoard board(RailTopology t, int station, int nowSec, int perLine) {
        Map<String, List<StationBoard.Departure>> byLine = new LinkedHashMap<>();
        int horizon = nowSec + HORIZON_SEC;

        for (int sr = t.stopRoutesBegin(station); sr < t.stopRoutesEnd(station); sr++) {
            int route = t.stopRoute(sr);
            int pos = t.stopRoutePos(sr);
            int terminusPos = t.numStops(route) - 1;
            if (pos >= terminusPos) {
                continue; // a terminus only receives trains; nothing departs onward
            }
            String line = t.lineNames()[t.routeLine(route)];
            String dest = t.stationName(t.routeStop(route, terminusPos));
            int trips = t.numTrips(route);
            for (int tr = 0; tr < trips; tr++) {
                int dep = t.depSec(route, tr, pos);
                if (dep >= nowSec && dep < horizon) {
                    byLine.computeIfAbsent(line, k -> new ArrayList<>())
                            .add(new StationBoard.Departure(dep, dep - nowSec, dest));
                }
            }
        }

        List<StationBoard.LineDepartures> lines = new ArrayList<>();
        byLine.forEach((line, deps) -> {
            deps.sort(Comparator.comparingInt(StationBoard.Departure::depSec));
            List<StationBoard.Departure> next = new ArrayList<>();
            StationBoard.Departure prev = null;
            for (StationBoard.Departure d : deps) {
                // Collapse identical departures (same second + terminus) from a line's
                // many overlapping stop-patterns into a single train.
                if (prev != null && prev.depSec() == d.depSec()
                        && prev.destination().equals(d.destination())) {
                    continue;
                }
                next.add(d);
                prev = d;
                if (next.size() == perLine) {
                    break;
                }
            }
            lines.add(new StationBoard.LineDepartures(line, next));
        });
        lines.sort(Comparator.comparing(StationBoard.LineDepartures::line));
        return new StationBoard(station, t.stationName(station), nowSec, lines);
    }
}
