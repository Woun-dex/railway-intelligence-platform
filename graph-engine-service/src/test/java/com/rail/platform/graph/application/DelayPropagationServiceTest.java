package com.rail.platform.graph.application;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.rail.platform.graph.domain.model.DelaySource;
import com.rail.platform.graph.domain.model.PropagationResult;
import com.rail.platform.graph.support.FixedTopologyRepository;
import com.rail.platform.graph.support.MockNetworks;

class DelayPropagationServiceTest {

    private DelayPropagationService service() {
        return new DelayPropagationService(new FixedTopologyRepository(MockNetworks.slackChain()), 64);
    }

    private Map<Integer, PropagationResult> propagate(int delay) {
        List<PropagationResult> r = service().propagate(new DelaySource("trip-1", 0, delay, "evt-1", 1_000L));
        return r.stream().collect(Collectors.toMap(PropagationResult::stationIndex, Function.identity()));
    }

    @Test
    void absorbsSlackAlongTheChain() {
        // Each running+dwell hop absorbs 60 + 30 = 90s.
        Map<Integer, PropagationResult> byStation = propagate(300);

        assertThat(byStation.get(0).propagatedDelaySec()).isEqualTo(300); // source
        assertThat(byStation.get(1).propagatedDelaySec()).isEqualTo(210);
        assertThat(byStation.get(2).propagatedDelaySec()).isEqualTo(120);
        assertThat(byStation.get(3).propagatedDelaySec()).isEqualTo(30);
        assertThat(byStation.get(3).absorbedSlackSec()).isEqualTo(270);
    }

    @Test
    void flagsMissedConnectionWhenDelayExceedsTransferSlack() {
        Map<Integer, PropagationResult> byStation = propagate(300);

        // Transfer 2->4 has 100s slack; residual at 2 is 120 > 100 -> missed.
        PropagationResult viaTransfer = byStation.get(4);
        assertThat(viaTransfer.propagatedDelaySec()).isEqualTo(20);
        assertThat(viaTransfer.missedConnection()).isTrue();
        assertThat(viaTransfer.absorbedSlackSec()).isEqualTo(280);

        // A running hop is not a missed connection.
        assertThat(byStation.get(2).missedConnection()).isFalse();
    }

    @Test
    void cascadeDiesWhereSlackFullyAbsorbsTheDelay() {
        // 100s delay: 0 -> 1 leaves 10s; 1 -> 2 would need 90s, fully absorbed.
        Map<Integer, PropagationResult> byStation = propagate(100);

        assertThat(byStation).containsOnlyKeys(0, 1);
        assertThat(byStation.get(1).propagatedDelaySec()).isEqualTo(10);
    }

    @Test
    void ignoresNonPositiveDelayAndUnknownStation() {
        assertThat(service().propagate(new DelaySource("t", 0, 0, "e", 0L))).isEmpty();
        assertThat(service().propagate(new DelaySource("t", 0, -120, "e", 0L))).isEmpty();
        assertThat(service().propagate(new DelaySource("t", 999, 300, "e", 0L))).isEmpty();
    }
}
