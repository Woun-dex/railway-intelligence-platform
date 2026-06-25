package com.rail.platform.graph;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Engine 1 — the memory-localized graph computation layer.
 *
 * <p>On startup it loads the rail topology (real Transilien GTFS) into an
 * embedded Hazelcast IMDG and caches a flat, primitive-array representation in
 * local heap. It then:
 * <ul>
 *   <li>consumes {@code rail.raw.position} and propagates delays through the
 *       event-activity network (Engine 1a), emitting {@code rail.graph.cascade};</li>
 *   <li>answers RAPTOR journey queries over the in-heap timetable (Engine 1b);</li>
 *   <li>exposes the network as a matrix at {@code /graph/matrix} with a live
 *       viewer at {@code /viz.html}.</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class GraphEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(GraphEngineApplication.class, args);
    }
}
