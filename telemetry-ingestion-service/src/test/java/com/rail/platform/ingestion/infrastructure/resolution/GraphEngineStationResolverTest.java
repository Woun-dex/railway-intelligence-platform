package com.rail.platform.ingestion.infrastructure.resolution;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

/** Resolution rules: exact match, feed-prefix suffix fallback, and misses. */
class GraphEngineStationResolverTest {

    private GraphEngineStationResolver resolver() {
        var r = new GraphEngineStationResolver(WebClient.builder(), "http://localhost:8091", 300_000);
        r.seed(Map.of("IDFM:463685", 42, "8775810", 7, "StopArea:59455", 100));
        return r;
    }

    @Test
    void resolvesExactStopId() {
        assertThat(resolver().resolve("IDFM:463685")).isEqualTo(42);
    }

    @Test
    void resolvesViaSuffixAfterLastColon() {
        // a fully-qualified SIRI ref whose suffix matches a known stop_id
        assertThat(resolver().resolve("StopPoint:Q:8775810")).isEqualTo(7);
    }

    @Test
    void returnsNullForUnknownAndBlank() {
        var r = resolver();
        assertThat(r.resolve("IDFM:000000")).isNull();
        assertThat(r.resolve(null)).isNull();
        assertThat(r.resolve("   ")).isNull();
    }

    @Test
    void emptyCacheResolvesToNull() {
        var r = new GraphEngineStationResolver(WebClient.builder(), "http://localhost:8091", 300_000);
        assertThat(r.resolve("IDFM:463685")).isNull();
        assertThat(r.size()).isZero();
    }
}
