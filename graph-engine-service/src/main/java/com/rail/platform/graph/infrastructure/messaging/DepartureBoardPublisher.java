package com.rail.platform.graph.infrastructure.messaging;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.rail.platform.graph.application.DepartureBoardService;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.model.StationBoard;
import com.rail.platform.graph.domain.port.out.DisplayPublisherPort;
import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.schemas.display.Channel;
import com.rail.platform.schemas.display.DisplayMessage;

/**
 * Real-time passenger-information feed: on a fixed cadence, compute the next
 * departures for the network's stations and publish them to {@code rail.display}
 * as {@link DisplayMessage} events — one per departure, keyed by line ("canal").
 *
 * <p>This is the event-driven replacement for a synchronous query: the display
 * system (the viewer, an MQTT bridge, …) <em>subscribes</em> to the stream rather
 * than polling. Departure times are carried as absolute epoch millis in
 * {@code valid_to_ms} so any consumer can render a live local-time countdown.
 */
@Component
public class DepartureBoardPublisher {

    private static final Logger log = LoggerFactory.getLogger(DepartureBoardPublisher.class);
    private static final ZoneId PARIS = ZoneId.of("Europe/Paris");

    private final TopologyRepository repository;
    private final DepartureBoardService boards;
    private final DisplayPublisherPort publisher;
    private final int perLine;
    private final int stationLimit;
    private final AtomicLong seq = new AtomicLong();

    public DepartureBoardPublisher(TopologyRepository repository, DepartureBoardService boards,
                                   DisplayPublisherPort publisher,
                                   @Value("${rail.display.per-line:5}") int perLine,
                                   @Value("${rail.display.station-limit:0}") int stationLimit) {
        this.repository = repository;
        this.boards = boards;
        this.publisher = publisher;
        this.perLine = perLine;
        this.stationLimit = stationLimit;
    }

    @Scheduled(fixedDelayString = "${rail.display.interval-ms:15000}",
            initialDelayString = "${rail.display.initial-delay-ms:8000}")
    public void publishBoards() {
        RailTopology t = repository.current();
        if (t == null) {
            return;
        }
        ZonedDateTime startOfDay = LocalDate.now(PARIS).atStartOfDay(PARIS);
        long cycleMs = System.currentTimeMillis();
        int nowSec = (int) ((cycleMs - startOfDay.toInstant().toEpochMilli()) / 1000);

        int count = (stationLimit > 0) ? Math.min(stationLimit, t.stationCount()) : t.stationCount();
        List<DisplayMessage> batch = new ArrayList<>();
        for (int s = 0; s < count; s++) {
            StationBoard board = boards.board(t, s, nowSec, perLine);
            for (StationBoard.LineDepartures line : board.lines()) {
                for (StationBoard.Departure d : line.departures()) {
                    long depMs = startOfDay.toInstant().toEpochMilli() + d.depSec() * 1000L;
                    batch.add(DisplayMessage.newBuilder()
                            .setMessageId(s + "/" + line.line() + "/" + d.depSec())
                            .setTripId(line.line())                       // canal key
                            .setChannel(Channel.CHANNEL_PIV)
                            .setSubject("piv/idf/" + s + "/" + line.line())
                            .setHeadline(d.destination())
                            .setBody(line.line() + " → " + d.destination())
                            .setLocale("fr-FR")
                            .setValidFromMs(cycleMs)
                            .setValidToMs(depMs)
                            .setDisplaySeq(seq.incrementAndGet())
                            .build());
                }
            }
        }
        if (batch.isEmpty()) {
            return;
        }
        publisher.publish(batch)
                .doOnError(e -> log.warn("display board publish failed: {}", e.toString()))
                .onErrorComplete()
                .subscribe();
        log.debug("published {} departure messages for {} stations to rail.display", batch.size(), count);
    }
}
