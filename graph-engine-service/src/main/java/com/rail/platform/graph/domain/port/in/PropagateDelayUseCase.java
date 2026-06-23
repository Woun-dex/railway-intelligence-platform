package com.rail.platform.graph.domain.port.in;

import java.util.List;

import com.rail.platform.graph.domain.model.DelaySource;
import com.rail.platform.graph.domain.model.PropagationResult;

/**
 * Engine 1a inbound port: propagate a primary delay through the event-activity
 * network and return every downstream node affected after slack absorption.
 */
public interface PropagateDelayUseCase {

    List<PropagationResult> propagate(DelaySource source);
}
