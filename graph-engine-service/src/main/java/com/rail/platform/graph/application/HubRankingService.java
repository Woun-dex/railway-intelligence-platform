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
     * Standard PageRank with a uniform teleport vector — the structural hub score.
     */
    public double[] pageRank(RailTopology t) {
        return pageRank(t, null);
    }

    /**
     * Power-iteration PageRank over the directed CSR adjacency with a configurable
     * teleport vector. Passing {@code personalization} (a probability vector that
     * sums to 1) computes <em>personalized</em> PageRank — the PCC's operational
     * weights bias rank toward boosted lines/stations and away from suppressed
     * ones, so the hub vector reflects the live picture. {@code null} = uniform
     * teleport (identical to the structural ranking). Dangling nodes (no out-edges)
     * teleport along the same vector so total rank is conserved.
     */
    public double[] pageRank(RailTopology t, double[] personalization) {
        int n = t.stationCount();
        double[] pr = new double[n];
        if (n == 0) {
            return pr;
        }
        double[] teleport = personalization;
        if (teleport == null) {
            teleport = new double[n];
            java.util.Arrays.fill(teleport, 1.0 / n);
        }
        System.arraycopy(teleport, 0, pr, 0, n);

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
            for (int i = 0; i < n; i++) {
                next[i] = (1.0 - damping) * teleport[i]
                        + damping * (next[i] + dangling * teleport[i]);
            }
            pr = next;
        }
        return pr;
    }
}
