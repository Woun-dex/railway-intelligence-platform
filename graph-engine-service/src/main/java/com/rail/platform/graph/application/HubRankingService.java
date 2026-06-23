package com.rail.platform.graph.application;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.RailTopology;

/**
 * Computes the offline hub vector (PageRank) over the station adjacency graph.
 *
 * <p>"Offline" means precomputed once when a topology is loaded and cached in the
 * snapshot — never on the hot path. Engine 1a uses the resulting score to
 * prioritize propagation through structurally central hubs (e.g. Paris
 * Saint-Lazare, La&nbsp;Défense), so the most network-significant consequences of
 * a delay are computed first.
 */
@Service
public class HubRankingService {

    private final double damping;
    private final int iterations;

    public HubRankingService(
            @Value("${rail.graph.pagerank.damping:0.85}") double damping,
            @Value("${rail.graph.pagerank.iterations:50}") int iterations) {
        this.damping = damping;
        this.iterations = iterations;
    }

    /**
     * Power-iteration PageRank over the directed CSR adjacency. Dangling nodes
     * (no out-edges) teleport uniformly so total rank is conserved.
     */
    public double[] pageRank(RailTopology t) {
        int n = t.stationCount();
        double[] pr = new double[n];
        if (n == 0) {
            return pr;
        }
        java.util.Arrays.fill(pr, 1.0 / n);

        for (int it = 0; it < iterations; it++) {
            double[] next = new double[n];
            double dangling = 0.0;
            for (int j = 0; j < n; j++) {
                int begin = t.adjBegin(j);
                int end = t.adjEnd(j);
                int deg = end - begin;
                if (deg == 0) {
                    dangling += pr[j];
                } else {
                    double share = pr[j] / deg;
                    for (int e = begin; e < end; e++) {
                        next[t.adjTarget(e)] += share;
                    }
                }
            }
            double base = (1.0 - damping) / n + damping * dangling / n;
            for (int i = 0; i < n; i++) {
                next[i] = base + damping * next[i];
            }
            pr = next;
        }
        return pr;
    }
}
