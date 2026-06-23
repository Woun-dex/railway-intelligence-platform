package com.rail.platform.ingestion.infrastructure.normalization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.schemas.telemetry.PositionEvent;


class ProtobufPayloadMapperTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ProtobufPayloadMapper parser = new ProtobufPayloadMapper();

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
}
