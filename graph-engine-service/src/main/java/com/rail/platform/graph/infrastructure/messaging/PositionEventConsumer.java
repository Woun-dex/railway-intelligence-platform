package com.rail.platform.graph.infrastructure.messaging;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.domain.model.DelaySource;
import com.rail.platform.graph.domain.model.PropagationResult;
import com.rail.platform.graph.domain.port.in.PropagateDelayUseCase;
import com.rail.platform.graph.domain.port.out.CascadePublisherPort;
import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.telemetry.PositionEvent;

import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

/**
 * Consumes {@code rail.raw.position}, runs Engine 1a on each delayed position,
 * and emits the resulting cascade.
 *
 * <p>Propagation (CPU-bound) runs on a parallel scheduler so the Kafka poll loop
 * is never blocked; offsets are acknowledged only after the cascade is published,
 * giving at-least-once delivery. A single bad record is dead-ended (logged) rather
 * than poisoning the stream, and the whole subscription retries with backoff if
 * the broker is briefly unavailable.
 */
@Component
public class PositionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PositionEventConsumer.class);
    private static final int CONCURRENCY = 64;

    private final ReceiverOptions<String, PositionEvent> receiverOptions;
    private final PropagateDelayUseCase propagate;
    private final CascadePublisherPort publisher;
    private final TopologyRepository topology;

    private Disposable subscription;

    public PositionEventConsumer(ReceiverOptions<String, PositionEvent> receiverOptions,
                                 PropagateDelayUseCase propagate,
                                 CascadePublisherPort publisher,
                                 TopologyRepository topology) {
        this.receiverOptions = receiverOptions;
        this.propagate = propagate;
        this.publisher = publisher;
        this.topology = topology;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        KafkaReceiver<String, PositionEvent> receiver = KafkaReceiver.create(receiverOptions);
        subscription = receiver.receive()
                .flatMap(record -> Mono
                        .fromCallable(() -> buildCascade(record.value()))
                        .subscribeOn(Schedulers.parallel())
                        .flatMap(events -> events.isEmpty()
                                ? Mono.empty()
                                : publisher.publish(record.key(), events))
                        .onErrorResume(err -> {
                            log.warn("propagation failed for key={}: {}", record.key(), err.toString());
                            return Mono.empty();
                        })
                        .thenReturn(record.receiverOffset()), CONCURRENCY)
                .doOnNext(ReceiverOffset::acknowledge)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe(
                        offset -> { },
                        err -> log.error("position consumer terminated", err));
        log.info("Position consumer started (group subscription active)");
    }

    /** Maps a PositionEvent into a cascade (empty if there is nothing to propagate). */
    private List<CascadeEvent> buildCascade(PositionEvent ev) {
        if (ev == null) {
            return List.of();
        }
        DelaySource source = new DelaySource(
                ev.getTripId(),
                ev.getStationId(),
                (int) Math.round(ev.getDelaySeconds()),
                ev.getEventId(),
                ev.getEventTimeMs());

        List<PropagationResult> results = propagate.propagate(source);
        if (results.isEmpty()) {
            return List.of();
        }
        String graphVersion = topology.version();
        long computeTime = System.currentTimeMillis();
        return results.stream()
                .map(r -> CascadeEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setOriginEventId(ev.getEventId())
                        .setTripId(ev.getTripId())
                        .setStationId(r.stationIndex())
                        .setPropagatedDelaySeconds(r.propagatedDelaySec())
                        .setAbsorbedSlackSeconds(r.absorbedSlackSec())
                        .setMissedConnection(r.missedConnection())
                        .setGraphVersion(graphVersion == null ? "" : graphVersion)
                        .setEventTimeMs(ev.getEventTimeMs())
                        .setComputeTimeMs(computeTime)
                        .build())
                .toList();
    }

    @PreDestroy
    void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}
