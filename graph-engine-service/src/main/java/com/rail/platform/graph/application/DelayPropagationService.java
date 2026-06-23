package com.rail.platform.graph.application;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.rail.platform.graph.domain.model.DelaySource;
import com.rail.platform.graph.domain.model.PropagationResult;
import com.rail.platform.graph.domain.model.RailTopology;
import com.rail.platform.graph.domain.port.in.PropagateDelayUseCase;
import com.rail.platform.graph.domain.port.out.TopologyRepository;

/**
 * Engine 1a — deterministic delay propagation over the event-activity network.
 *
 * <p>From a primary delay {@code δ₀} at the source station, the delay is pushed
 * downstream and absorbed by the recovery margins on each edge:
 * <pre>
 *   running + dwell : δ' = max(0, δ − runSlack − dwellSlack)
 *   transfer        : δ' = max(0, δ − transferSlack);  missed if δ &gt; transferSlack
 * </pre>
 * The cascade dies out naturally where slack absorbs the residual. Expansion is
 * a label-correcting relaxation keeping the <em>worst-case</em> residual at each
 * node (events carry absolute values, so worst-case is the safe merge); the
 * frontier is ordered by residual then by the PageRank hub score, so the most
 * delayed and most network-central consequences are computed first.
 *
 * <p>Reads the in-heap topology directly — no grid round-trip on the hot path.
 */
@Service
public class DelayPropagationService implements PropagateDelayUseCase {

    private final TopologyRepository repository;
    private final int maxHops;

    public DelayPropagationService(TopologyRepository repository,
                                   @Value("${rail.graph.propagation.max-hops:64}") int maxHops) {
        this.repository = repository;
        this.maxHops = maxHops;
    }

    // Label fields packed into an int[]: {station, residual, hop, absorbed, missed}.
    private static final int STATION = 0, RESIDUAL = 1, HOP = 2, ABSORBED = 3, MISSED = 4;

    @Override
    public List<PropagationResult> propagate(DelaySource source) {
        RailTopology t = repository.current();
        if (t == null || !t.isStation(source.stationIndex()) || source.delaySeconds() <= 0) {
            return List.of();
        }
        int origin = source.stationIndex();
        int delay0 = source.delaySeconds();

        Map<Integer, int[]> best = new HashMap<>();
        PriorityQueue<int[]> pq = new PriorityQueue<>(
                Comparator.<int[]>comparingInt(a -> -a[RESIDUAL])
                        .thenComparing(a -> -t.pagerank(a[STATION])));

        int[] root = {origin, delay0, 0, 0, 0};
        best.put(origin, root);
        pq.add(root);

        while (!pq.isEmpty()) {
            int[] cur = pq.poll();
            int station = cur[STATION];
            int residual = cur[RESIDUAL];
            int[] rec = best.get(station);
            if (rec[RESIDUAL] != residual) {
                continue; // stale label — a worse residual has superseded it
            }
            if (cur[HOP] >= maxHops) {
                continue;
            }
            // Running + dwell edges.
            for (int e = t.adjBegin(station); e < t.adjEnd(station); e++) {
                int target = t.adjTarget(e);
                int absorb = t.adjRunSlackSec(e) + t.dwellSlackSec(target);
                int childResidual = Math.max(0, residual - absorb);
                if (childResidual <= 0) {
                    continue; // fully absorbed — cascade terminates on this branch
                }
                relax(pq, best, target, childResidual,
                        cur[HOP] + 1, cur[ABSORBED] + (residual - childResidual), false);
            }
            // Foot-transfer edges (missed-connection detection).
            for (int x = t.xferBegin(station); x < t.xferEnd(station); x++) {
                int target = t.xferTarget(x);
                int slack = t.xferSlackSec(x);
                int childResidual = Math.max(0, residual - slack);
                if (childResidual <= 0) {
                    continue;
                }
                relax(pq, best, target, childResidual,
                        cur[HOP] + 1, cur[ABSORBED] + (residual - childResidual), residual > slack);
            }
        }

        List<PropagationResult> out = new ArrayList<>(best.size());
        for (int[] v : best.values()) {
            out.add(new PropagationResult(v[STATION], v[RESIDUAL], v[ABSORBED], v[MISSED] == 1, v[HOP]));
        }
        out.sort(Comparator.comparingInt(PropagationResult::hop)
                .thenComparingInt(PropagationResult::stationIndex));
        return out;
    }

    private static void relax(PriorityQueue<int[]> pq, Map<Integer, int[]> best, int target,
                              int residual, int hop, int absorbed, boolean missed) {
        int[] existing = best.get(target);
        if (existing != null && residual <= existing[RESIDUAL]) {
            return; // keep the worst-case (largest) residual
        }
        int[] label = {target, residual, hop, absorbed, missed ? 1 : 0};
        best.put(target, label);
        pq.add(label);
    }
}
