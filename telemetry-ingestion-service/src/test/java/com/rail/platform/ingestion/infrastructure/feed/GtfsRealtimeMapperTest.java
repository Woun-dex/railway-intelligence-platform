package com.rail.platform.ingestion.infrastructure.feed;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedHeader;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.TripUpdate;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeEvent;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

/** Maps hand-built GTFS-RT FeedMessages → canonical position frames. */
class GtfsRealtimeMapperTest {

    private final GtfsRealtimeMapper mapper = new GtfsRealtimeMapper(new ObjectMapper());

    private static FeedMessage wrap(FeedEntity... entities) {
        FeedMessage.Builder b = FeedMessage.newBuilder()
                .setHeader(FeedHeader.newBuilder().setGtfsRealtimeVersion("2.0").setTimestamp(1_700_000_000));
        for (FeedEntity e : entities) b.addEntity(e);
        return b.build();
    }

    @Test
    void mapsTripUpdateToStationAnchoredFrame() {
        var tu = TripUpdate.newBuilder()
                .setTrip(TripDescriptor.newBuilder().setTripId("RER-A:T1").setRouteId("A"))
                .addStopTimeUpdate(StopTimeUpdate.newBuilder()
                        .setStopId("IDFM:463685")
                        .setDeparture(StopTimeEvent.newBuilder().setDelay(240)))
                .build();
        List<ObjectNode> frames = mapper.toPositionFrames(
                wrap(FeedEntity.newBuilder().setId("e1").setTripUpdate(tu).build()), "GTFS-RT/TU");

        assertThat(frames).hasSize(1);
        ObjectNode f = frames.get(0);
        assertThat(f.get("trip_id").asText()).isEqualTo("RER-A:T1");
        assertThat(f.get("stop_id").asText()).isEqualTo("IDFM:463685");
        assertThat(f.get("delay_seconds").asInt()).isEqualTo(240);
        assertThat(f.get("line_id").asText()).isEqualTo("A");
        assertThat(f.has("lat")).isFalse(); // no GPS in a TripUpdate
    }

    @Test
    void prefersDepartureDelayAndFirstDelayedStop() {
        var tu = TripUpdate.newBuilder()
                .setTrip(TripDescriptor.newBuilder().setTripId("T2"))
                .addStopTimeUpdate(StopTimeUpdate.newBuilder().setStopId("S-no-delay"))
                .addStopTimeUpdate(StopTimeUpdate.newBuilder().setStopId("S-arr")
                        .setArrival(StopTimeEvent.newBuilder().setDelay(60)))
                .build();
        var frames = mapper.toPositionFrames(
                wrap(FeedEntity.newBuilder().setId("e").setTripUpdate(tu).build()), "GTFS-RT/TU");
        assertThat(frames).hasSize(1);
        assertThat(frames.get(0).get("stop_id").asText()).isEqualTo("S-arr");
        assertThat(frames.get(0).get("delay_seconds").asInt()).isEqualTo(60);
    }

    @Test
    void mapsVehiclePositionToCoordinateFrame() {
        var vp = VehiclePosition.newBuilder()
                .setTrip(TripDescriptor.newBuilder().setTripId("T3").setRouteId("B"))
                .setPosition(Position.newBuilder().setLatitude(48.8584f).setLongitude(2.2945f))
                .setStopId("IDFM:71410")
                .build();
        var frames = mapper.toPositionFrames(
                wrap(FeedEntity.newBuilder().setId("v").setVehicle(vp).build()), "GTFS-RT/VP");
        assertThat(frames).hasSize(1);
        ObjectNode f = frames.get(0);
        assertThat(f.get("lat").asDouble()).isCloseTo(48.8584, org.assertj.core.data.Offset.offset(1e-4));
        assertThat(f.get("stop_id").asText()).isEqualTo("IDFM:71410");
    }

    @Test
    void dropsTripUpdateWithNoDelayedStop() {
        var tu = TripUpdate.newBuilder()
                .setTrip(TripDescriptor.newBuilder().setTripId("T4"))
                .addStopTimeUpdate(StopTimeUpdate.newBuilder().setStopId("S1"))
                .build();
        assertThat(mapper.toPositionFrames(
                wrap(FeedEntity.newBuilder().setId("e").setTripUpdate(tu).build()), "GTFS-RT/TU")).isEmpty();
    }
}
