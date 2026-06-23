package com.rail.platform.ingestion.application;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.ingestion.domain.model.DeadLetter;
import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;
import com.rail.platform.ingestion.domain.model.RawTelemetry;
import com.rail.platform.ingestion.domain.model.TelemetryKind;
import com.rail.platform.ingestion.domain.model.TripId;
import com.rail.platform.ingestion.domain.port.out.DeadLetterPort;
import com.rail.platform.ingestion.domain.port.out.TelemetryNormalizer;
import com.rail.platform.ingestion.domain.port.out.TelemetryPublisherPort;
import com.rail.platform.schemas.telemetry.PositionEvent;

import static org.assertj.core.api.Assertions.assertThat;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Use-case test driven entirely through the domain ports with in-memory fakes —
 * no Spring context, no Kafka, no Jackson. This is the payoff of the hexagonal
 * arrangement: the core business rule (normalize → publish, else dead-letter)
 * is verified in microseconds and in complete isolation from infrastructure.
 */
class TelemetryIngestionServiceTest {

    private final RecordingPublisher publisher = new RecordingPublisher();
    private final RecordingDeadLetter deadLetters = new RecordingDeadLetter();

    @Test
    void admitsAValidFrameToThePublisherAndNeverDeadLetters() {
        TelemetryNormalizer ok = raw -> new NormalizedTelemetry(
                raw.kind(), TripId.of("T1"), PositionEvent.newBuilder().setTripId("T1").build());
        var service = new TelemetryIngestionService(ok, publisher, deadLetters);

        service.ingest(RawTelemetry.of(TelemetryKind.POSITION, "{}".getBytes())).block();

        assertThat(publisher.published).hasSize(1);
        assertThat(publisher.published.get(0).key().value()).isEqualTo("T1");
        assertThat(deadLetters.captured).isEmpty();
    }

    @Test
    void quarantinesAnUnnormalizableFrameAndNeverPublishes() {
        TelemetryNormalizer rejecting = raw -> {
            throw new PayloadNormalizationException("missing mandatory field 'trip_id'");
        };
        var service = new TelemetryIngestionService(rejecting, publisher, deadLetters);

        service.ingest(RawTelemetry.of(TelemetryKind.POSITION, "{bad".getBytes())).block();

        assertThat(publisher.published).isEmpty();
        assertThat(deadLetters.captured).hasSize(1);
        assertThat(deadLetters.captured.get(0).reason()).contains("trip_id");
    }

    @Test
    void streamPathPublishesGoodFramesAndQuarantinesBadOnes() {
        // odd-indexed frames fail normalization → DLQ; even-indexed succeed.
        var counter = new java.util.concurrent.atomic.AtomicInteger();
        TelemetryNormalizer selective = raw -> {
            int n = counter.getAndIncrement();
            if (n % 2 == 1) throw new PayloadNormalizationException("bad #" + n);
            return new NormalizedTelemetry(raw.kind(), TripId.of("T" + n),
                    PositionEvent.newBuilder().setTripId("T" + n).build());
        };
        var service = new TelemetryIngestionService(selective, publisher, deadLetters);

        var frames = Flux.range(0, 10)
                .map(i -> RawTelemetry.of(TelemetryKind.POSITION, ("{}" + i).getBytes()));
        service.ingestStream(frames).block();

        assertThat(publisher.published).hasSize(5);   // the 5 even frames
        assertThat(deadLetters.captured).hasSize(5);  // the 5 odd frames
    }

    // ---- in-memory port fakes ----------------------------------------------
    private static final class RecordingPublisher implements TelemetryPublisherPort {
        final List<NormalizedTelemetry> published = new ArrayList<>();
        @Override public Mono<Void> publish(NormalizedTelemetry telemetry) {
            published.add(telemetry);
            return Mono.empty();
        }
        @Override public Mono<Void> publishAll(Flux<NormalizedTelemetry> stream) {
            return stream.doOnNext(published::add).then();
        }
    }

    private static final class RecordingDeadLetter implements DeadLetterPort {
        final List<DeadLetter> captured = new ArrayList<>();
        @Override public Mono<Void> send(DeadLetter deadLetter) {
            captured.add(deadLetter);
            return Mono.empty();
        }
    }
}
