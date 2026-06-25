package com.rail.platform.graph.domain.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Display-only track geometry: the real polyline (from GTFS {@code shapes.txt})
 * running between two adjacent stations, so the map can draw the actual track
 * curvature instead of a straight station-to-station chord.
 *
 * <p>Keyed by the directed station pair {@code (from << 32) | to}; the value is a
 * flattened {@code [lon, lat, lon, lat, ...]} polyline (GeoJSON coordinate order).
 * The map is empty when the feed carries no shapes, in which case consumers fall
 * back to straight segments. This is never consulted on the routing hot path —
 * it travels with the topology only for the viewer and Hazelcast recovery.
 */
public final class NetworkGeometry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<Long, float[]> segments;

    public NetworkGeometry(Map<Long, float[]> segments) {
        this.segments = segments;
    }

    public static NetworkGeometry empty() {
        return new NetworkGeometry(Map.of());
    }

    public static long key(int from, int to) {
        return (((long) from) << 32) | (to & 0xffffffffL);
    }

    public boolean isEmpty() {
        return segments.isEmpty();
    }

    public int segmentCount() {
        return segments.size();
    }

    /** Flattened {@code [lon, lat, ...]} polyline from {@code from} to {@code to}, or {@code null}. */
    public float[] between(int from, int to) {
        return segments.get(key(from, to));
    }

    /** Raw backing map (directed-pair key → flattened polyline) for bulk consumers. */
    public Map<Long, float[]> segments() {
        return segments;
    }
}
