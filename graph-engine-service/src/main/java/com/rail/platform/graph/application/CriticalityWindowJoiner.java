package com.rail.platform.graph.application;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.rail.platform.schemas.prediction.CascadeEvent;
import com.rail.platform.schemas.prediction.PredictionEvent;

/**
 * Sliding-window stream joiner keyed by {@code station_id}.
 *
 * <p>The deterministic cascade and the STGCN escalation forecast arrive on two
 * independent topics with no shared correlation id, so they are joined on the
 * station they describe, but only when their event times fall within a strict
 * window {@code W}. The STGCN forecast is a property of a station/region, so a
 * single fresh prediction may join with several cascades hitting that station;
 * a cascade is consumed once joined to avoid duplicate emission.
 *
 * <p>State is two small per-station maps (latest prediction, pending cascade).
 * {@link #evict(long)} drops entries older than {@code W} and, when
 * {@code emitUnjoined} is set, surfaces cascades that expired without a
 * prediction so the score still flows (with {@code I_P = 0}) if Engine 3 is down.
 * All mutating methods are synchronized — event volume is modest and the maps
 * are tiny (bounded by station count).
 */
public class CriticalityWindowJoiner {

    /** A successful (or expired-unjoined) join ready to be scored. */
    public record Joined(CascadeEvent cascade, PredictionEvent prediction, long joinLagMs) {
        public boolean predictionJoined() {
            return prediction != null;
        }
    }

    private record Stamped<T>(T event, long ts) { }

    private final long windowMs;
    private final boolean emitUnjoined;
    private final Map<Integer, Stamped<PredictionEvent>> predictions = new ConcurrentHashMap<>();
    private final Map<Integer, Stamped<CascadeEvent>> cascades = new ConcurrentHashMap<>();

    public CriticalityWindowJoiner(long windowMs, boolean emitUnjoined) {
        this.windowMs = windowMs;
        this.emitUnjoined = emitUnjoined;
    }

    /** Offer a cascade; returns a join if a fresh prediction for its station exists. */
    public synchronized List<Joined> offerCascade(CascadeEvent c) {
        int station = c.getStationId();
        long ts = eventTime(c.getEventTimeMs());
        Stamped<PredictionEvent> p = predictions.get(station);
        if (p != null && Math.abs(ts - p.ts()) <= windowMs) {
            return List.of(new Joined(c, p.event(), Math.abs(ts - p.ts())));
        }
        cascades.put(station, new Stamped<>(c, ts)); // wait for a prediction
        return List.of();
    }

    /** Offer a prediction; returns a join if a pending cascade for its station is in-window. */
    public synchronized List<Joined> offerPrediction(PredictionEvent p) {
        int station = p.getStationId();
        long ts = eventTime(p.getEventTimeMs());
        predictions.put(station, new Stamped<>(p, ts)); // becomes the latest, reusable
        Stamped<CascadeEvent> c = cascades.get(station);
        if (c != null && Math.abs(ts - c.ts()) <= windowMs) {
            cascades.remove(station);
            return List.of(new Joined(c.event(), p, Math.abs(ts - c.ts())));
        }
        return List.of();
    }

    /**
     * Evict entries whose timestamp is older than the window relative to
     * {@code now}. Expired cascades are returned as unjoined scores when
     * {@code emitUnjoined} is enabled; expired predictions are simply dropped.
     */
    public synchronized List<Joined> evict(long now) {
        List<Joined> expired = new ArrayList<>();
        cascades.entrySet().removeIf(e -> {
            boolean stale = now - e.getValue().ts() > windowMs;
            if (stale && emitUnjoined) {
                expired.add(new Joined(e.getValue().event(), null, -1));
            }
            return stale;
        });
        predictions.entrySet().removeIf(e -> now - e.getValue().ts() > windowMs);
        return expired;
    }

    public int pendingCascades() {
        return cascades.size();
    }

    public int livePredictions() {
        return predictions.size();
    }

    /** Event time, falling back to wall clock for unset (0) timestamps. */
    private static long eventTime(long eventTimeMs) {
        return eventTimeMs > 0 ? eventTimeMs : System.currentTimeMillis();
    }
}
