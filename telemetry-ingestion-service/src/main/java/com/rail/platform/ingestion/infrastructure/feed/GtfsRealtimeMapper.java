package com.rail.platform.ingestion.infrastructure.feed;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

/**
 * Translates a GTFS-Realtime {@link FeedMessage} into the gateway's canonical
 * position-frame JSON (the same shape the REST gateway accepts), which then flows
 * through the normal normalization → resolver → publish path.
 *
 * <p>Two entity kinds are mapped:
 * <ul>
 *   <li><b>TripUpdate</b> → carries {@code stop_id} + a per-stop {@code delay} but
 *       no GPS; emitted as a station-anchored frame (the resolver maps
 *       {@code stop_id} → index). The representative stop is the first
 *       {@code StopTimeUpdate} bearing a delay (departure preferred over arrival).</li>
 *   <li><b>VehiclePosition</b> → carries GPS (and often a current {@code stop_id});
 *       emitted as a coordinate frame.</li>
 * </ul>
 * Pure and side-effect free so it is unit-testable from a hand-built FeedMessage.
 */
@Component
public class GtfsRealtimeMapper {

    private final ObjectMapper json;

    public GtfsRealtimeMapper(ObjectMapper json) {
        this.json = json;
    }

    public List<ObjectNode> toPositionFrames(FeedMessage msg, String sourceFeed) {
        long headerMs = msg.hasHeader() && msg.getHeader().hasTimestamp()
                ? msg.getHeader().getTimestamp() * 1000L : System.currentTimeMillis();
        List<ObjectNode> frames = new ArrayList<>();
        for (FeedEntity e : msg.getEntityList()) {
            if (e.hasTripUpdate()) {
                ObjectNode f = fromTripUpdate(e.getTripUpdate(), headerMs, sourceFeed);
                if (f != null) frames.add(f);
            } else if (e.hasVehicle()) {
                ObjectNode f = fromVehicle(e.getVehicle(), headerMs, sourceFeed);
                if (f != null) frames.add(f);
            }
        }
        return frames;
    }

    private ObjectNode fromTripUpdate(TripUpdate tu, long headerMs, String sourceFeed) {
        String tripId = tu.getTrip().getTripId();
        if (tripId.isEmpty()) {
            return null; // no partition key → cannot publish
        }
        StopTimeUpdate chosen = null;
        Integer delay = null;
        for (StopTimeUpdate stu : tu.getStopTimeUpdateList()) {
            if (stu.hasDeparture() && stu.getDeparture().hasDelay()) {
                chosen = stu;
                delay = stu.getDeparture().getDelay();
                break;
            }
            if (chosen == null && stu.hasArrival() && stu.getArrival().hasDelay()) {
                chosen = stu;
                delay = stu.getArrival().getDelay();
            }
        }
        if (chosen == null || chosen.getStopId().isEmpty()) {
            return null; // nothing actionable (no delayed stop with an id)
        }
        long ts = tu.hasTimestamp() ? tu.getTimestamp() * 1000L : headerMs;
        ObjectNode f = json.createObjectNode();
        f.put("trip_id", tripId);
        if (!tu.getTrip().getRouteId().isEmpty()) f.put("line_id", tu.getTrip().getRouteId());
        f.put("stop_id", chosen.getStopId());
        f.put("delay_seconds", delay == null ? 0 : delay);
        f.put("event_time_ms", ts);
        f.put("source_feed", sourceFeed);
        return f;
    }

    private ObjectNode fromVehicle(VehiclePosition vp, long headerMs, String sourceFeed) {
        String tripId = vp.getTrip().getTripId();
        if (tripId.isEmpty() || !vp.hasPosition()) {
            return null;
        }
        long ts = vp.hasTimestamp() ? vp.getTimestamp() * 1000L : headerMs;
        ObjectNode f = json.createObjectNode();
        f.put("trip_id", tripId);
        if (!vp.getTrip().getRouteId().isEmpty()) f.put("line_id", vp.getTrip().getRouteId());
        f.put("lat", vp.getPosition().getLatitude());
        f.put("lon", vp.getPosition().getLongitude());
        if (vp.getPosition().hasBearing()) f.put("bearing", vp.getPosition().getBearing());
        if (vp.getPosition().hasSpeed()) f.put("speed_kmh", vp.getPosition().getSpeed() * 3.6);
        if (!vp.getStopId().isEmpty()) f.put("stop_id", vp.getStopId());
        f.put("event_time_ms", ts);
        f.put("source_feed", sourceFeed);
        return f;
    }
}
