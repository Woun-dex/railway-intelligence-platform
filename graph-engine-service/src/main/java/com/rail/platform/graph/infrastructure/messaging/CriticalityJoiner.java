package com.rail.platform.graph.infrastructure.messaging;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.google.protobuf.Message;
import com.rail.platform.graph.application.CriticalityScoringService;
import com.rail.platform.graph.application.CriticalityState;
import com.rail.platform.graph.application.CriticalityWindowJoiner;
import com.rail.platform.graph.application.CriticalityWindowJoiner.Joined;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.schemas.display.CriticalityEvent;
import com.rail.platform.schemas.display.CriticalityScore;
import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.prediction.PredictionEvent;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

/**
 * Engine-1 streaming joiner. Consumes the deterministic cascade
 * ({@code rail.graph.cascade}) and the STGCN escalation forecast
 * ({@code rail.predictions.stgcn}) on two reactive subscriptions, merges them by
 * station inside a strict sliding window ({@link CriticalityWindowJoiner}),
 * scores the unified Criticality ({@link CriticalityScoringService}), and emits a
 * {@link CriticalityEvent} to {@code rail.criticality.scored}.
 *
 * <p>Each subscription does its CPU-light work on a parallel scheduler and
 * acknowledges offsets only after the resulting scores are published
 * (at-least-once). A periodic sweep evicts window entries that aged out and, when
 * configured, emits cascade-only scores so the pipeline still produces output if
 * Engine 3 is offline.
 */
@Component
public class CriticalityJoiner {

    private static final Logger log = LoggerFactory.getLogger(CriticalityJoiner.class);
    private static final int CONCURRENCY = 32;

    private final ReceiverOptions<String, CascadeEvent> cascadeOptions;
    private final ReceiverOptions<String, PredictionEvent> predictionOptions;
    private final KafkaSender<String, Message> sender;
    private final CriticalityScoringService scoring;
    private final CriticalityState state;
    private final TopologyRepository topology;
    private final String outputTopic;
    private final long evictMs;
    private final CriticalityWindowJoiner joiner;

    private Disposable cascadeSub;
    private Disposable predictionSub;
    private Disposable evictSub;

    public CriticalityJoiner(ReceiverOptions<String, CascadeEvent> cascadeReceiverOptions,
                             ReceiverOptions<String, PredictionEvent> predictionReceiverOptions,
                             @Qualifier("criticalitySender") KafkaSender<String, Message> criticalitySender,
                             CriticalityScoringService scoring,
                             CriticalityState state,
                             TopologyRepository topology,
                             @Value("${rail.kafka.topics.criticality}") String outputTopic,
                             @Value("${rail.criticality.window-ms:60000}") long windowMs,
                             @Value("${rail.criticality.evict-ms:5000}") long evictMs,
                             @Value("${rail.criticality.emit-unjoined:true}") boolean emitUnjoined) {
        this.cascadeOptions = cascadeReceiverOptions;
        this.predictionOptions = predictionReceiverOptions;
        this.sender = criticalitySender;
        this.scoring = scoring;
        this.state = state;
        this.topology = topology;
        this.outputTopic = outputTopic;
        this.evictMs = evictMs;
        this.joiner = new CriticalityWindowJoiner(windowMs, emitUnjoined);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        cascadeSub = subscribe(KafkaReceiver.create(cascadeOptions), this::onCascade, "cascade");
        predictionSub = subscribe(KafkaReceiver.create(predictionOptions), this::onPrediction, "prediction");
        evictSub = Flux.interval(Duration.ofMillis(evictMs))
                .concatMap(tick -> publish(joiner.evict(System.currentTimeMillis())))
                .onErrorContinue((e, o) -> log.warn("eviction sweep error: {}", e.toString()))
                .subscribe();
        log.info("Criticality joiner started (cascade ⋈ prediction → {})", outputTopic);
    }

    private <T> Disposable subscribe(KafkaReceiver<String, T> receiver,
                                     java.util.function.Function<T, List<Joined>> handler,
                                     String name) {
        return receiver.receive()
                .flatMap(record -> Mono
                        .fromCallable(() -> handler.apply(record.value()))
                        .subscribeOn(Schedulers.parallel())
                        .flatMap(this::publish)
                        .onErrorResume(err -> {
                            log.warn("{} join failed: {}", name, err.toString());
                            return Mono.empty();
                        })
                        .thenReturn(record.receiverOffset()), CONCURRENCY)
                .doOnNext(ReceiverOffset::acknowledge)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe(o -> { }, err -> log.error("{} subscription terminated", name, err));
    }

    private List<Joined> onCascade(CascadeEvent c) {
        return c == null ? List.of() : joiner.offerCascade(c);
    }

    private List<Joined> onPrediction(PredictionEvent p) {
        return p == null ? List.of() : joiner.offerPrediction(p);
    }

    /** Score every join and publish the resulting CriticalityEvents. */
    private Mono<Void> publish(List<Joined> joins) {
        if (joins.isEmpty()) {
            return Mono.empty();
        }
        Flux<SenderRecord<String, Message, String>> records = Flux.fromIterable(joins)
                .map(this::toEvent)
                .doOnNext(state::record)
                .map(e -> SenderRecord.create(
                        new ProducerRecord<>(outputTopic, e.getTripId(), (Message) e), e.getEventId()));
        return sender.send(records)
                .doOnNext(result -> {
                    if (result.exception() != null) {
                        log.warn("criticality publish failed for {}: {}",
                                result.correlationMetadata(), result.exception().getMessage());
                    }
                })
                .then();
    }

    private CriticalityEvent toEvent(Joined j) {
        CascadeEvent c = j.cascade();
        PredictionEvent p = j.prediction();
        CriticalityScore score = scoring.score(c, p);
        RailTopology t = topology.current();
        String stationName = (t != null && t.isStation(c.getStationId())) ? t.stationName(c.getStationId()) : "";
        String graphVersion = t != null && t.graphVersion() != null ? t.graphVersion() : c.getGraphVersion();
        return CriticalityEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setTripId(c.getTripId() == null ? "" : c.getTripId())
                .setStationId(c.getStationId())
                .setStationName(stationName == null ? "" : stationName)
                .setScore(score)
                .setCascadeEventId(c.getEventId() == null ? "" : c.getEventId())
                .setPredictionEventId(p == null || p.getEventId() == null ? "" : p.getEventId())
                .setJoinLagMs(j.joinLagMs())
                .setPredictionJoined(j.predictionJoined())
                .setGraphVersion(graphVersion == null ? "" : graphVersion)
                .setEventTimeMs(c.getEventTimeMs())
                .setDecisionTimeMs(System.currentTimeMillis())
                .build();
    }

    @PreDestroy
    void stop() {
        dispose(cascadeSub);
        dispose(predictionSub);
        dispose(evictSub);
    }

    private static void dispose(Disposable d) {
        if (d != null && !d.isDisposed()) {
            d.dispose();
        }
    }
}
