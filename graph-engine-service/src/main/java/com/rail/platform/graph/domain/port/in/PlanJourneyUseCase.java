package com.rail.platform.graph.domain.port.in;

import com.rail.platform.graph.domain.model.JourneyPlan;
import com.rail.platform.graph.domain.model.JourneyQuery;

/**
 * Engine 1b inbound port: compute the Pareto-optimal journeys for an
 * origin-destination query over the current timetable.
 */
public interface PlanJourneyUseCase {

    JourneyPlan plan(JourneyQuery query);
}
