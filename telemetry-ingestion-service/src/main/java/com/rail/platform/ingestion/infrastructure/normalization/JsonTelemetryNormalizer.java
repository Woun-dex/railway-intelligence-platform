package com.rail.platform.ingestion.infrastructure.normalization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.rail.platform.ingestion.domain.exception.PayloadNormalizationException;
import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;
import com.rail.platform.ingestion.domain.model.RawTelemetry;
import com.rail.platform.ingestion.domain.model.TripId;
import com.rail.platform.ingestion.domain.port.out.TelemetryNormalizer;
import com.rail.platform.schemas.telemetry.IncidentEvent;
import com.rail.platform.schemas.telemetry.PositionEvent;
import com.rail.platform.schemas.telemetry.SignallingEvent;
import org.springframework.stereotype.Component;

/**
 * Infrastructure adapter implementing the {@link TelemetryNormalizer} ACL port.
 *
 * <p>It owns the JSON decoding (Jackson) and delegates per-kind field mapping to
 * {@link ProtobufPayloadMapper}. Both the JSON parse and the field mapping
 * surface failures as {@link PayloadNormalizationException}, so the application
 * service has a single failure mode to quarantine. The resulting
 * {@link NormalizedTelemetry} carries the published-language payload and the
 * {@link TripId} extracted from it.
 */
@Component
public class JsonTelemetryNormalizer implements TelemetryNormalizer {

    private final ObjectMapper objectMapper;
    private final ProtobufPayloadMapper mapper;

    public JsonTelemetryNormalizer(ObjectMapper objectMapper, ProtobufPayloadMapper mapper) {
        this.objectMapper = objectMapper;
        this.mapper = mapper;
    }

    @Override
    public NormalizedTelemetry normalize(RawTelemetry raw) {
        JsonNode node;
        try {
            node = objectMapper.readTree(raw.body());
        } catch (Exception e) {
            throw new PayloadNormalizationException("malformed JSON: " + e.getMessage(), e);
        }
        if (node == null || node.isNull() || node.isMissingNode()) {
            throw new PayloadNormalizationException("empty or null JSON payload");
        }

        Message payload = switch (raw.kind()) {
            case POSITION   -> mapper.parsePosition(node);
            case SIGNALLING -> mapper.parseSignalling(node);
            case INCIDENT   -> mapper.parseIncident(node);
        };

        return new NormalizedTelemetry(raw.kind(), TripId.of(tripIdOf(payload)), payload);
    }

    private static String tripIdOf(Message payload) {
        if (payload instanceof PositionEvent p)   return p.getTripId();
        if (payload instanceof SignallingEvent s) return s.getTripId();
        if (payload instanceof IncidentEvent i)   return i.getTripId();
        throw new PayloadNormalizationException("unknown payload type: " + payload.getClass().getSimpleName());
    }
}
