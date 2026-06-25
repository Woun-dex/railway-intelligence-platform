package com.rail.platform.graph.application;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.rail.platform.graph.domain.model.RailTopology;

/**
 * Live operational overrides issued by the PCC (Poste de Commandement Centralisé).
 *
 * <p>Each override is a priority <em>multiplier</em> on a line ("canal") or a
 * single station: {@code 0} suppresses it (closure), {@code 1} is normal,
 * {@code >1} boosts it. These weights feed the hub-ranking teleport vector so the
 * PageRank hub scores shift to reflect the current operational picture instead of
 * a frozen boot-time value. Thread-safe; read on recompute, written by the PCC
 * command endpoint.
 */
@Component
public class OperationalState {

    private final Map<String, Double> lineWeights = new ConcurrentHashMap<>();
    private final Map<Integer, Double> stationWeights = new ConcurrentHashMap<>();

    /** Sets a line multiplier; {@code 1.0} clears the override. */
    public void setLine(String line, double weight) {
        if (weight == 1.0) {
            lineWeights.remove(line);
        } else {
            lineWeights.put(line, Math.max(0.0, weight));
        }
    }

    /** Sets a station multiplier; {@code 1.0} clears the override. */
    public void setStation(int station, double weight) {
        if (weight == 1.0) {
            stationWeights.remove(station);
        } else {
            stationWeights.put(station, Math.max(0.0, weight));
        }
    }

    public void clear() {
        lineWeights.clear();
        stationWeights.clear();
    }

    public double lineWeight(String line) {
        return lineWeights.getOrDefault(line, 1.0);
    }

    public double stationWeight(int station) {
        return stationWeights.getOrDefault(station, 1.0);
    }

    public Map<String, Double> lines() {
        return Map.copyOf(lineWeights);
    }

    public Map<Integer, Double> stations() {
        return Map.copyOf(stationWeights);
    }

    public boolean isEmpty() {
        return lineWeights.isEmpty() && stationWeights.isEmpty();
    }

    /**
     * Builds the normalized personalization (teleport) vector for {@code t}: a
     * station's weight is its own override times the average override of the lines
     * serving it. Sums to 1; falls back to uniform when everything cancels.
     */
    public double[] personalization(RailTopology t) {
        int n = t.stationCount();
        double[] p = new double[n];
        double sum = 0.0;
        for (int i = 0; i < n; i++) {
            double lineFactor = 1.0;
            int[] ls = t.linesOf(i);
            if (ls.length > 0) {
                double acc = 0.0;
                for (int li : ls) {
                    acc += lineWeight(t.lineNames()[li]);
                }
                lineFactor = acc / ls.length;
            }
            double w = Math.max(0.0, stationWeight(i) * lineFactor);
            p[i] = w;
            sum += w;
        }
        if (sum <= 0.0) {
            Arrays.fill(p, 1.0 / n);
            return p;
        }
        for (int i = 0; i < n; i++) {
            p[i] /= sum;
        }
        return p;
    }
}
