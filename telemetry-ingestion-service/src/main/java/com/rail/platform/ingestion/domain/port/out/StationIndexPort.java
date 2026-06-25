package com.rail.platform.ingestion.domain.port.out;

/**
 * Resolves a feed-native stop identifier (GTFS {@code stop_id} / SIRI
 * {@code MonitoringRef}) to the engine's dense integer station index.
 *
 * <p>Real-time feeds address stops by their published GTFS {@code stop_id}, but
 * the graph engine and the STGCN spatial operator index stations by a contiguous
 * integer. This outbound port lets the normalization layer translate identifiers
 * without knowing where the mapping comes from (it is served by the graph engine's
 * {@code /graph/stopmap}). Returns {@code null} when the id is unknown, so the
 * caller can leave the station unset rather than mis-address an event.
 */
public interface StationIndexPort {

    Integer resolve(String stopId);
}
