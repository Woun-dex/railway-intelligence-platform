package com.rail.platform.graph.domain.port.in;

import com.rail.platform.graph.domain.model.JourneyPath;
import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;

/**
 * Engine 1b inbound port: compute the Pareto-optimal journeys for an
 * origin-destination query over the current timetable.
 */
public interface PlanJourneyUseCase {

    JourneyPlan plan(JourneyQuery query);

    /**
     * Reconstruct the concrete earliest-arrival journey (leg-by-leg path) for the
     * query — used by the viewer to draw the route on the map.
     */
    JourneyPath planPath(JourneyQuery query);
}
