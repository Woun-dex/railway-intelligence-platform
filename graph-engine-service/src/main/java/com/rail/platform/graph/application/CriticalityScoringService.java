package com.rail.platform.graph.application;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.port.out.TopologyRepository;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.schemas.display.CriticalityScore;
import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.prediction.PredictionEvent;

/**
 * Computes the unified Criticality Score that fuses the deterministic cascade
 * (Engine 1a) with the STGCN escalation forecast (Engine 3):
 *
 * <pre>
 *   CS = beta * ( w_T * dT_surplus + w_V * P_missed + w_P * I_P )
 * </pre>
 *
 * where {@code dT_surplus} is the propagated residual delay in minutes,
 * {@code P_missed} the missed-connection indicator, {@code I_P} the predicted
 * escalation ratio, and {@code beta} a hub-criticality multiplier derived from
 * the station's PageRank (so disruptions at structural hubs outrank peripheral
 * ones). A cascade with no joined prediction scores with {@code I_P = 0}.
 */
@Service
public class CriticalityScoringService {

    private final TopologyRepository topology;
    private final double wT;
    private final double wV;
    private final double wP;
    private final double betaGain;

    /** Cached per topology version so we don't rescan PageRank on every event. */
    private volatile String prCacheVersion = null;
    private volatile double prMax = 1.0;

    public CriticalityScoringService(TopologyRepository topology,
                                     @Value("${rail.criticality.weights.t:1.0}") double wT,
                                     @Value("${rail.criticality.weights.v:2.0}") double wV,
                                     @Value("${rail.criticality.weights.p:3.0}") double wP,
                                     @Value("${rail.criticality.beta-gain:1.0}") double betaGain) {
        this.topology = topology;
        this.wT = wT;
        this.wV = wV;
        this.wP = wP;
        this.betaGain = betaGain;
    }

    /**
     * Score a cascade joined with an (optional) prediction. {@code prediction}
     * may be {@code null} for a cascade-only score (escalation term = 0).
     */
    public CriticalityScore score(CascadeEvent cascade, PredictionEvent prediction) {
        double dtSurplus = Math.max(0.0, cascade.getPropagatedDelaySeconds()) / 60.0;
        double pMissed = cascade.getMissedConnection() ? 1.0 : 0.0;
        double iP = prediction == null ? 0.0
                : clamp01(prediction.getEscalationRatio());
        double beta = hubMultiplier(cascade.getStationId());

        double total = beta * (wT * dtSurplus + wV * pMissed + wP * iP);
        return CriticalityScore.newBuilder()
                .setTImpact(dtSurplus)
                .setPMiss(pMissed)
                .setIEscalate(iP)
                .setVContext(0.0)
                .setBetaHub(beta)
                .setTotal(total)
                .build();
    }

    /** beta = 1 + gain * pagerank(station) / max_pagerank, clamped to >= 1. */
    private double hubMultiplier(int stationId) {
        RailTopology t = topology.current();
        if (t == null || !t.isStation(stationId)) {
            return 1.0;
        }
        refreshPrMax(t);
        double pr = t.pagerank(stationId);
        return 1.0 + betaGain * (prMax > 0 ? pr / prMax : 0.0);
    }

    private void refreshPrMax(RailTopology t) {
        String v = t.graphVersion();
        if (v != null && v.equals(prCacheVersion)) {
            return;
        }
        double max = 0.0;
        for (int i = 0; i < t.stationCount(); i++) {
            max = Math.max(max, t.pagerank(i));
        }
        this.prMax = max <= 0 ? 1.0 : max;
        this.prCacheVersion = v;
    }

    private static double clamp01(double v) {
        if (Double.isNaN(v)) {
            return 0.0;
        }
        return v < 0 ? 0 : (v > 1 ? 1 : v);
    }
}
