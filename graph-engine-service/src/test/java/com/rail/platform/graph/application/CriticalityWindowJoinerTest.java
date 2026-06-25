package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.rail.platform.graph.application.CriticalityWindowJoiner.Joined;
import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.prediction.PredictionEvent;

/**
 * Verifies the sliding-window join semantics that back the milestone DoD:
 * deterministic cascade ⋈ STGCN prediction, keyed by station, only when their
 * event times fall within the strict window.
 */
class CriticalityWindowJoinerTest {

    private static final long WINDOW_MS = 60_000;

    private static CascadeEvent cascade(int station, double delaySec, boolean missed, long tMs) {
        return CascadeEvent.newBuilder()
                .setEventId("c-" + tMs).setTripId("RER-A:T1").setStationId(station)
                .setPropagatedDelaySeconds(delaySec).setMissedConnection(missed)
                .setEventTimeMs(tMs).build();
    }

    private static PredictionEvent prediction(int station, double iP, long tMs) {
        return PredictionEvent.newBuilder()
                .setEventId("p-" + tMs).setTripId("RER-A:T9").setStationId(station)
                .setEscalationRatio(iP).setEventTimeMs(tMs).build();
    }

    @Test
    void joinsCascadeThenPredictionWithinWindow() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        assertThat(j.offerCascade(cascade(5, 300, false, 1_000))).isEmpty(); // waits
        List<Joined> joined = j.offerPrediction(prediction(5, 0.4, 30_000));  // 29s later
        assertThat(joined).hasSize(1);
        assertThat(joined.get(0).predictionJoined()).isTrue();
        assertThat(joined.get(0).joinLagMs()).isEqualTo(29_000);
        assertThat(j.pendingCascades()).isZero(); // cascade consumed
    }

    @Test
    void joinsPredictionThenCascadeWithinWindow() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        assertThat(j.offerPrediction(prediction(7, 0.2, 5_000))).isEmpty();
        List<Joined> joined = j.offerCascade(cascade(7, 120, true, 40_000));
        assertThat(joined).hasSize(1);
        assertThat(joined.get(0).prediction().getEscalationRatio()).isEqualTo(0.2);
    }

    @Test
    void doesNotJoinOutsideWindow() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        j.offerCascade(cascade(3, 300, false, 1_000));
        List<Joined> joined = j.offerPrediction(prediction(3, 0.5, 90_000)); // 89s > 60s
        assertThat(joined).isEmpty();
    }

    @Test
    void doesNotJoinDifferentStations() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        j.offerCascade(cascade(3, 300, false, 1_000));
        assertThat(j.offerPrediction(prediction(4, 0.5, 2_000))).isEmpty();
    }

    @Test
    void freshPredictionJoinsMultipleCascades() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        j.offerPrediction(prediction(9, 0.3, 10_000));
        assertThat(j.offerCascade(cascade(9, 100, false, 20_000))).hasSize(1);
        assertThat(j.offerCascade(cascade(9, 200, true, 30_000))).hasSize(1); // reuses prediction
    }

    @Test
    void evictionEmitsUnjoinedCascadeWhenEnabled() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, true);
        j.offerCascade(cascade(2, 300, false, 1_000));
        List<Joined> expired = j.evict(1_000 + WINDOW_MS + 1);
        assertThat(expired).hasSize(1);
        assertThat(expired.get(0).predictionJoined()).isFalse();
        assertThat(expired.get(0).joinLagMs()).isEqualTo(-1);
    }

    @Test
    void evictionDropsUnjoinedCascadeWhenDisabled() {
        var j = new CriticalityWindowJoiner(WINDOW_MS, false);
        j.offerCascade(cascade(2, 300, false, 1_000));
        assertThat(j.evict(1_000 + WINDOW_MS + 1)).isEmpty();
        assertThat(j.pendingCascades()).isZero();
    }
}
