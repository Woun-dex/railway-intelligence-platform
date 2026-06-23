package com.rail.platform.ingestion.infrastructure.normalization;

import com.fasterxml.jackson.databind.JsonNode;
import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.schemas.telemetry.BlockState;
import com.rail.platform.schemas.telemetry.GeoPoint;
import com.rail.platform.schemas.telemetry.IncidentCategory;
import com.rail.platform.schemas.telemetry.IncidentEvent;
import com.rail.platform.schemas.telemetry.IncidentSeverity;
import com.rail.platform.schemas.telemetry.InterlockingStatus;
import com.rail.platform.schemas.telemetry.PositionEvent;
import com.rail.platform.schemas.telemetry.SignallingEvent;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Anti-Corruption Layer mapper: heterogeneous raw JSON → strict Protobuf
 * published-language contracts.
 *
 * <p>Upstream feeds differ in casing and field names (SIRI-ET uses
 * {@code VehicleJourneyRef}/{@code RecordedAtTime}; GTFS-RT uses
 * {@code trip_id}/{@code timestamp}). The mapper probes a list of accepted
 * aliases per field and tolerates both flat and nested position objects. It
 * fails fast with {@link PayloadNormalizationException} only when mandatory
 * identity is absent — that record is then dead-lettered, never silently
 * dropped.
 *
 * <p>This is an infrastructure detail of the {@link JsonTelemetryNormalizer};
 * it deals in {@code JsonNode} and Protobuf builders so the domain never has to.
 */
@Component
public class ProtobufPayloadMapper {

    // ---- field alias tables (SIRI-ET / GTFS-RT variations) -----------------
    private static final String[] TRIP_ID   = {"trip_id", "tripId", "tripRef", "DatedVehicleJourneyRef", "VehicleJourneyRef"};
    private static final String[] VEHICLE_ID = {"vehicle_id", "vehicleId", "vehicleRef", "VehicleRef"};
    private static final String[] LINE_ID   = {"line_id", "lineId", "lineRef", "LineRef", "route_id", "routeId"};
    private static final String[] LAT        = {"latitude", "lat", "Latitude"};
    private static final String[] LON        = {"longitude", "lon", "lng", "Longitude"};
    private static final String[] DELAY      = {"delay_seconds", "delaySeconds", "delay", "Delay"};
    private static final String[] BEARING    = {"bearing", "heading", "Bearing"};
    private static final String[] SPEED      = {"speed_kmh", "speed", "Velocity"};
    private static final String[] STATION_ID = {"station_id", "stationId", "stopId", "MonitoringRef"};
    private static final String[] NEXT_STOP  = {"next_stop_id", "nextStopId", "nextStopRef"};
    private static final String[] TIMESTAMP  = {"event_time_ms", "timestamp", "ts", "RecordedAtTime"};
    private static final String[] SOURCE     = {"source_feed", "source", "producer"};
    private static final String[] GRAPH_VER  = {"graph_version", "graphVersion"};

    private static final String[] BLOCK_ID   = {"block_id", "blockId", "trackCircuitId", "tcId"};
    private static final String[] BLOCK_STATE = {"block_state", "blockState", "occupancy"};
    private static final String[] INTERLOCK  = {"interlocking_status", "interlockingStatus", "interlocking"};
    private static final String[] SIGNAL_ID  = {"owning_signal_id", "signalId", "signalRef"};

    private static final String[] CATEGORY   = {"category", "incidentType", "type"};
    private static final String[] SEVERITY   = {"severity", "Severity", "level"};
    private static final String[] DESCRIPTION = {"description", "summary", "Summary"};
    private static final String[] CLEARANCE  = {"estimated_clearance_ms", "estimatedClearance", "expectedEndTime", "ValidUntilTime"};
    private static final String[] START_TIME = {"start_time_ms", "startTime", "CreationTime"};

    // ========================================================================
    // PositionEvent
    // ========================================================================
    public PositionEvent parsePosition(JsonNode node) {
        String tripId = requireText(node, TRIP_ID, "trip_id");

        // Position may be flat (lat/lon at root) or nested under position/Position/Location.
        JsonNode posNode = firstChild(node, "position", "Position", "Location", "VehicleLocation");
        Double lat = readDouble(posNode, LAT);
        Double lon = readDouble(posNode, LON);
        if (lat == null) lat = readDouble(node, LAT);
        if (lon == null) lon = readDouble(node, LON);
        if (lat == null || lon == null) {
            throw new PayloadNormalizationException("PositionEvent missing coordinates for trip_id=" + tripId);
        }

        long now = System.currentTimeMillis();
        PositionEvent.Builder b = PositionEvent.newBuilder()
                .setEventId(fastEventId())
                .setTripId(tripId)
                .setVehicleId(textOrEmpty(node, VEHICLE_ID))
                .setLineId(textOrEmpty(node, LINE_ID))
                .setPosition(GeoPoint.newBuilder().setLatitude(lat).setLongitude(lon).build())
                .setDelaySeconds(readDelaySeconds(node))
                .setSourceFeed(textOr(node, SOURCE, "UNKNOWN"))
                .setGraphVersion(textOrEmpty(node, GRAPH_VER))
                .setEventTimeMs(readTimestampMs(node, now))
                .setIngestTimeMs(now);

        Double bearing = readDouble(node, BEARING);
        if (bearing != null) b.setBearing(bearing);
        Double speed = readDouble(node, SPEED);
        if (speed != null) b.setSpeedKmh(speed);
        Integer station = readInt(node, STATION_ID);
        if (station != null) b.setStationId(station);
        Integer next = readInt(node, NEXT_STOP);
        if (next != null) b.setNextStopId(next);

        return b.build();
    }

    // ========================================================================
    // SignallingEvent
    // ========================================================================
    public SignallingEvent parseSignalling(JsonNode node) {
        String tripId = requireText(node, TRIP_ID, "trip_id");
        String blockId = requireText(node, BLOCK_ID, "block_id");
        long now = System.currentTimeMillis();

        SignallingEvent.Builder b = SignallingEvent.newBuilder()
                .setEventId(fastEventId())
                .setTripId(tripId)
                .setBlockId(blockId)
                .setBlockState(parseBlockState(textOrEmpty(node, BLOCK_STATE)))
                .setInterlockingStatus(parseInterlocking(textOrEmpty(node, INTERLOCK)))
                .setOwningSignalId(textOrEmpty(node, SIGNAL_ID))
                .setGraphVersion(textOrEmpty(node, GRAPH_VER))
                .setEventTimeMs(readTimestampMs(node, now))
                .setIngestTimeMs(now);

        Integer station = readInt(node, STATION_ID);
        if (station != null) b.setStationId(station);
        return b.build();
    }

    // ========================================================================
    // IncidentEvent
    // ========================================================================
    public IncidentEvent parseIncident(JsonNode node) {
        long now = System.currentTimeMillis();
        IncidentEvent.Builder b = IncidentEvent.newBuilder()
                .setEventId(fastEventId())
                .setCategory(parseCategory(textOrEmpty(node, CATEGORY)))
                .setSeverity(parseSeverity(textOrEmpty(node, SEVERITY)))
                .setDescription(textOrEmpty(node, DESCRIPTION))
                .setGraphVersion(textOrEmpty(node, GRAPH_VER))
                .setEventTimeMs(readTimestampMs(node, now))
                .setStartTimeMs(readTimestampMs(node, START_TIME, now))
                .setIngestTimeMs(now);

        // Impacted lines / stations (accept array or single scalar)
        JsonNode lines = firstChild(node, "impacted_line_ids", "impactedLines", "AffectsLines", "lines");
        collectStrings(lines, b::addImpactedLineIds);
        JsonNode stations = firstChild(node, "impacted_station_ids", "impactedStations", "stations");
        collectInts(stations, b::addImpactedStationIds);

        // Clearance window
        Long clearance = readEpochMs(node, CLEARANCE);
        if (clearance != null) {
            b.setEstimatedClearanceMs(clearance).setClearanceEstimateKnown(true);
        }

        // Partition key: trip_id when attributable, else the primary line id.
        String tripId = textOrEmpty(node, TRIP_ID);
        if (tripId.isEmpty() && b.getImpactedLineIdsCount() > 0) {
            tripId = b.getImpactedLineIds(0);
        }
        if (tripId.isEmpty()) {
            throw new PayloadNormalizationException("IncidentEvent has neither trip_id nor impacted line — cannot derive partition key");
        }
        b.setTripId(tripId);
        return b.build();
    }

    // ========================================================================
    // helpers
    // ========================================================================

    /**
     * Idempotency key for an event. Uses {@link ThreadLocalRandom} rather than
     * {@code UUID.randomUUID()} (which draws from a shared, lock-contended
     * {@code SecureRandom}) — an event id needs uniqueness, not cryptographic
     * unpredictability, and the contended path throttles the hot loop badly.
     */
    private static String fastEventId() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return new UUID(r.nextLong(), r.nextLong()).toString();
    }

    private static double readDelaySeconds(JsonNode node) {
        for (String key : DELAY) {
            JsonNode v = node.get(key);
            if (v == null || v.isNull()) continue;
            if (v.isNumber()) return v.asDouble();
            if (v.isTextual()) {
                String s = v.asText().trim();
                // SIRI ISO-8601 duration, e.g. "PT2M30S"
                if (s.startsWith("P") || s.startsWith("-P")) {
                    try {
                        return Duration.parse(s).toMillis() / 1000.0;
                    } catch (DateTimeParseException ignored) { /* fall through */ }
                }
                try {
                    return Double.parseDouble(s);
                } catch (NumberFormatException ignored) { /* fall through */ }
            }
        }
        return 0.0;
    }

    private static long readTimestampMs(JsonNode node, long fallback) {
        return readTimestampMs(node, TIMESTAMP, fallback);
    }

    private static long readTimestampMs(JsonNode node, String[] keys, long fallback) {
        Long v = readEpochMs(node, keys);
        return v != null ? v : fallback;
    }

    private static Long readEpochMs(JsonNode node, String[] keys) {
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v == null || v.isNull()) continue;
            if (v.isNumber()) {
                long n = v.asLong();
                // Heuristic: treat 10-digit values as epoch seconds.
                return n < 100_000_000_000L ? n * 1000 : n;
            }
            if (v.isTextual()) {
                try {
                    return Instant.parse(v.asText().trim()).toEpochMilli();
                } catch (DateTimeParseException ignored) { /* fall through */ }
            }
        }
        return null;
    }

    private static BlockState parseBlockState(String s) {
        switch (s.trim().toUpperCase(Locale.ROOT)) {
            case "CLEAR": case "FREE": case "UNOCCUPIED": return BlockState.BLOCK_STATE_CLEAR;
            case "OCCUPIED": case "BUSY": return BlockState.BLOCK_STATE_OCCUPIED;
            case "RESERVED": case "LOCKED": return BlockState.BLOCK_STATE_RESERVED;
            case "FAILED": case "FAULT": return BlockState.BLOCK_STATE_FAILED;
            default: return BlockState.BLOCK_STATE_UNSPECIFIED;
        }
    }

    private static InterlockingStatus parseInterlocking(String s) {
        switch (s.trim().toUpperCase(Locale.ROOT)) {
            case "NORMAL": case "OK": return InterlockingStatus.INTERLOCKING_STATUS_NORMAL;
            case "DEGRADED": return InterlockingStatus.INTERLOCKING_STATUS_DEGRADED;
            case "FAILURE": case "FAILED": return InterlockingStatus.INTERLOCKING_STATUS_FAILURE;
            default: return InterlockingStatus.INTERLOCKING_STATUS_UNSPECIFIED;
        }
    }

    private static IncidentCategory parseCategory(String s) {
        switch (s.trim().toUpperCase(Locale.ROOT)) {
            case "INFRASTRUCTURE": case "TRACK": case "SIGNALLING": return IncidentCategory.INCIDENT_CATEGORY_INFRASTRUCTURE;
            case "ROLLING_STOCK": case "TRAIN": return IncidentCategory.INCIDENT_CATEGORY_ROLLING_STOCK;
            case "EXTERNAL": case "WEATHER": case "INTRUSION": return IncidentCategory.INCIDENT_CATEGORY_EXTERNAL;
            case "PASSENGER": case "MEDICAL": return IncidentCategory.INCIDENT_CATEGORY_PASSENGER;
            case "OPERATIONAL": case "STAFFING": return IncidentCategory.INCIDENT_CATEGORY_OPERATIONAL;
            default: return IncidentCategory.INCIDENT_CATEGORY_UNSPECIFIED;
        }
    }

    private static IncidentSeverity parseSeverity(String s) {
        switch (s.trim().toUpperCase(Locale.ROOT)) {
            case "MINOR": case "LOW": return IncidentSeverity.INCIDENT_SEVERITY_MINOR;
            case "MAJOR": case "HIGH": return IncidentSeverity.INCIDENT_SEVERITY_MAJOR;
            case "CRITICAL": case "SEVERE": return IncidentSeverity.INCIDENT_SEVERITY_CRITICAL;
            default: return IncidentSeverity.INCIDENT_SEVERITY_UNSPECIFIED;
        }
    }

    // ---- generic JSON accessors --------------------------------------------
    private static String requireText(JsonNode node, String[] keys, String label) {
        String v = textOrEmpty(node, keys);
        if (v.isEmpty()) {
            throw new PayloadNormalizationException("missing mandatory field '" + label + "'");
        }
        return v;
    }

    private static String textOrEmpty(JsonNode node, String[] keys) {
        return textOr(node, keys, "");
    }

    private static String textOr(JsonNode node, String[] keys, String dflt) {
        if (node == null) return dflt;
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v != null && !v.isNull() && v.isValueNode()) {
                String s = v.asText().trim();
                if (!s.isEmpty()) return s;
            }
        }
        return dflt;
    }

    private static Double readDouble(JsonNode node, String[] keys) {
        if (node == null) return null;
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v != null && !v.isNull()) {
                if (v.isNumber()) return v.asDouble();
                if (v.isTextual()) {
                    try { return Double.parseDouble(v.asText().trim()); }
                    catch (NumberFormatException ignored) { /* skip */ }
                }
            }
        }
        return null;
    }

    private static Integer readInt(JsonNode node, String[] keys) {
        Double d = readDouble(node, keys);
        return d == null ? null : (int) Math.round(d);
    }

    private static JsonNode firstChild(JsonNode node, String... keys) {
        if (node == null) return null;
        for (String key : keys) {
            JsonNode v = node.get(key);
            if (v != null && !v.isNull()) return v;
        }
        return null;
    }

    private static void collectStrings(JsonNode node, java.util.function.Consumer<String> sink) {
        if (node == null) return;
        if (node.isArray()) {
            node.forEach(n -> { if (n.isValueNode()) sink.accept(n.asText()); });
        } else if (node.isValueNode()) {
            sink.accept(node.asText());
        }
    }

    private static void collectInts(JsonNode node, java.util.function.IntConsumer sink) {
        if (node == null) return;
        if (node.isArray()) {
            node.forEach(n -> { if (n.isNumber()) sink.accept(n.asInt()); });
        } else if (node.isNumber()) {
            sink.accept(node.asInt());
        }
    }
}
