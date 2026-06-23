package com.rail.platform.ingestion.infrastructure.web;

import java.nio.charset.StandardCharsets;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rail.platform.ingestion.domain.model.RawTelemetry;
import com.rail.platform.ingestion.domain.model.TelemetryKind;
import com.rail.platform.ingestion.domain.port.in.IngestTelemetryUseCase;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Driving (inbound) adapter: the reactive HTTP gateway.
 *
 * <p>It depends only on the {@link IngestTelemetryUseCase} inbound port — it
 * knows nothing about Kafka, Protobuf, or normalization. Its sole job is to
 * adapt HTTP/JSON framing into the domain's {@link RawTelemetry} value object
 * and hand it to the use case. All HTTP-level concerns (status codes, JSON
 * (de)serialization) stay here, at the edge.
 *
 * <p>Single endpoints forward the body bytes verbatim (cheapest path, and the
 * exact bytes the DLQ needs on failure). Batch endpoints stream a JSON array as
 * {@code Flux<JsonNode>} and re-serialize each element to bytes at the edge.
 */
@RestController
@RequestMapping("/ingest")
public class TelemetryGatewayController {

    private final IngestTelemetryUseCase useCase;
    private final ObjectMapper objectMapper;

    public TelemetryGatewayController(IngestTelemetryUseCase useCase, ObjectMapper objectMapper) {
        this.useCase = useCase;
        this.objectMapper = objectMapper;
    }

    @PostMapping(value = "/position", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> ingestPosition(@RequestBody byte[] body) {
        return useCase.ingest(RawTelemetry.of(TelemetryKind.POSITION, body));
    }

    @PostMapping(value = "/signalling", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> ingestSignalling(@RequestBody byte[] body) {
        return useCase.ingest(RawTelemetry.of(TelemetryKind.SIGNALLING, body));
    }

    @PostMapping(value = "/incident", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> ingestIncident(@RequestBody byte[] body) {
        return useCase.ingest(RawTelemetry.of(TelemetryKind.INCIDENT, body));
    }

    @PostMapping(value = "/position/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> batchPositions(@RequestBody Flux<JsonNode> events) {
        return useCase.ingestStream(toRawStream(TelemetryKind.POSITION, events));
    }

    @PostMapping(value = "/signalling/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> batchSignalling(@RequestBody Flux<JsonNode> events) {
        return useCase.ingestStream(toRawStream(TelemetryKind.SIGNALLING, events));
    }

    @PostMapping(value = "/incident/batch", consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Void> batchIncidents(@RequestBody Flux<JsonNode> events) {
        return useCase.ingestStream(toRawStream(TelemetryKind.INCIDENT, events));
    }

    private Flux<RawTelemetry> toRawStream(TelemetryKind kind, Flux<JsonNode> events) {
        return events.map(node -> RawTelemetry.of(kind, toBytes(node)));
    }

    private byte[] toBytes(JsonNode node) {
        try {
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            // A node we just decoded should always re-serialize; if not, hand
            // the use case something that will fail normalization → DLQ.
            return ("{\"_unserializable\":\"" + e.getMessage() + "\"}").getBytes(StandardCharsets.UTF_8);
        }
    }
}
