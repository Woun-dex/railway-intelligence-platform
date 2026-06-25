package com.rail.platform.ingestion.infrastructure.normalization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.ingestion.domain.port.out.StationIndexPort;
import com.rail.platform.schemas.telemetry.PositionEvent;


class ProtobufPayloadMapperTest {

    private final ObjectMapper mapper = new ObjectMapper();
    // Stub resolver: a tiny GTFS stop_id → index map for the resolution tests.
    private final StationIndexPort stops = stopId -> Map.of(
            "IDFM:463685", 42, "8775810", 7).get(stopId);
    private final ProtobufPayloadMapper parser = new ProtobufPayloadMapper(stops);

    @Test
    void parsesGtfsRtStyleFlatPayload() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T1","vehicle_id":"V9","lat":48.8443,"lon":2.3743,
             "delay_seconds":125,"timestamp":1750000000}
            """);
        PositionEvent e = parser.parsePosition(node);
        assertThat(e.getTripId()).isEqualTo("T1");
        assertThat(e.getPosition().getLatitude()).isEqualTo(48.8443);
        assertThat(e.getDelaySeconds()).isEqualTo(125.0);
    }

    @Test
    void parsesSiriStyleNestedPayloadWithIsoDuration() throws Exception {
        var node = mapper.readTree("""
            {"VehicleJourneyRef":"T2","Position":{"Latitude":48.85,"Longitude":2.35},
             "Delay":"PT2M30S","RecordedAtTime":"2026-06-22T10:15:30Z"}
            """);
        PositionEvent e = parser.parsePosition(node);
        assertThat(e.getTripId()).isEqualTo("T2");
        assertThat(e.getDelaySeconds()).isEqualTo(150.0); // 2m30s
    }

    @Test
    void missingTripIdIsRejectedForDlq() throws Exception {
        var node = mapper.readTree("""
            {"lat":48.8,"lon":2.3}
            """);
        assertThatThrownBy(() -> parser.parsePosition(node))
                .isInstanceOf(PayloadNormalizationException.class);
    }

    @Test
    void missingCoordinatesIsRejectedForDlq() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T3"}
            """);
        assertThatThrownBy(() -> parser.parsePosition(node))
                .isInstanceOf(PayloadNormalizationException.class);
    }

    @Test
    void resolvesGtfsStopIdToStationIndex() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T4","lat":48.8,"lon":2.3,"stop_id":"IDFM:463685"}
            """);
        assertThat(parser.parsePosition(node).getStationId()).isEqualTo(42);
    }

    @Test
    void resolvesSiriMonitoringRefToStationIndex() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T5","lat":48.8,"lon":2.3,"MonitoringRef":"8775810"}
            """);
        assertThat(parser.parsePosition(node).getStationId()).isEqualTo(7);
    }

    @Test
    void numericStationIdIsTreatedAsExplicitIndex() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T6","lat":48.8,"lon":2.3,"station_id":95}
            """);
        assertThat(parser.parsePosition(node).getStationId()).isEqualTo(95);
    }

    @Test
    void unknownStopIdLeavesStationUnset() throws Exception {
        var node = mapper.readTree("""
            {"trip_id":"T7","lat":48.8,"lon":2.3,"stop_id":"IDFM:does-not-exist"}
            """);
        assertThat(parser.parsePosition(node).getStationId()).isEqualTo(0); // proto default
    }
}
