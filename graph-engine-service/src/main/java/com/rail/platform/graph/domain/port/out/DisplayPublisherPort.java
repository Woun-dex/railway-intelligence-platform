package com.rail.platform.graph.domain.port.out;

import java.util.List;

import com.rail.platform.schemas.display.DisplayMessage;

import reactor.core.publisher.Mono;

/**
 * Outbound port for emitting passenger-information messages. The published
 * language is the shared {@link DisplayMessage} contract on {@code rail.display};
 * each message is keyed by its line ("canal") so a line's messages keep their
 * order on a single partition.
 */
public interface DisplayPublisherPort {

    Mono<Void> publish(List<DisplayMessage> messages);
}
