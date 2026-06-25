package com.rail.platform.graph.infrastructure.messaging;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.rail.platform.schemas.display.DisplayMessage;

import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

/**
 * Bridges the {@code rail.display} Kafka topic to a hot {@link Flux} that the
 * HTTP layer fans out over Server-Sent Events. This is the read side of the
 * event-driven display: browsers subscribe to the stream instead of polling a
 * REST endpoint, and every departure they see has travelled through Kafka.
 *
 * <p>{@code directBestEffort} multicast: when no screen is connected the messages
 * are simply dropped (the next cadence refreshes them), so an idle bridge never
 * buffers.
 */
@Component
public class DisplayStreamBridge {

    private static final Logger log = LoggerFactory.getLogger(DisplayStreamBridge.class);

    private final ReceiverOptions<String, DisplayMessage> receiverOptions;
    private final Sinks.Many<DisplayMessage> sink = Sinks.many().multicast().directBestEffort();
    private Disposable subscription;

    public DisplayStreamBridge(ReceiverOptions<String, DisplayMessage> displayReceiverOptions) {
        this.receiverOptions = displayReceiverOptions;
    }

    /** Hot stream of every display message arriving on {@code rail.display}. */
    public Flux<DisplayMessage> stream() {
        return sink.asFlux();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        KafkaReceiver<String, DisplayMessage> receiver = KafkaReceiver.create(receiverOptions);
        subscription = receiver.receive()
                .doOnNext(record -> {
                    if (record.value() != null) {
                        sink.tryEmitNext(record.value());
                    }
                    record.receiverOffset().acknowledge();
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(2)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe(
                        r -> { },
                        err -> log.error("display stream bridge terminated", err));
        log.info("Display stream bridge started (tapping rail.display)");
    }

    @PreDestroy
    void stop() {
        if (subscription != null && !subscription.isDisposed()) {
            subscription.dispose();
        }
    }
}
