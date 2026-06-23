package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Random;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.rail.platform.graph.domain.model.JourneyQuery;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.support.FixedTopologyRepository;
import com.rail.platform.graph.support.MockNetworks;

/**
 * DoD micro-benchmark: a full-network RAPTOR recalculation must complete in
 * &lt; 12&nbsp;ms under load. Tagged {@code benchmark}; run on its own with
 * {@code mvn test -Dgroups=benchmark}.
 */
@Tag("benchmark")
class RaptorBenchmarkTest {

    @Test
    void fullNetworkRecalculationUnder12ms() {
        RailTopology t = MockNetworks.hundredStationStaircase();
        RaptorRouter router = new RaptorRouter(new FixedTopologyRepository(t));
        int n = t.stationCount();
        Random rnd = new Random(42);

        // Warm up the JIT and the per-thread scratch buffers.
        for (int i = 0; i < 5_000; i++) {
            router.plan(new JourneyQuery(rnd.nextInt(n), rnd.nextInt(n), rnd.nextInt(30_000), 6));
        }

        int iters = 10_000;
        long[] ns = new long[iters];
        for (int i = 0; i < iters; i++) {
            int o = rnd.nextInt(n);
            int d = rnd.nextInt(n);
            int dep = rnd.nextInt(30_000);
            long start = System.nanoTime();
            router.plan(new JourneyQuery(o, d, dep, 6));
            ns[i] = System.nanoTime() - start;
        }
        Arrays.sort(ns);
        double p50 = ns[iters / 2] / 1e6;
        double p95 = ns[(int) (iters * 0.95)] / 1e6;
        double p99 = ns[(int) (iters * 0.99)] / 1e6;
        System.out.printf("RAPTOR recompute latency: p50=%.4fms p95=%.4fms p99=%.4fms%n", p50, p95, p99);

        assertThat(p95).isLessThan(12.0);
        assertThat(p99).isLessThan(12.0);
    }
}
