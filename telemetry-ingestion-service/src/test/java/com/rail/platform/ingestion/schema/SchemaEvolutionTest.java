package com.rail.platform.ingestion.schema;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Schema-evolution guardrails (DoD: "backwards-compatibility validations").
 *
 * <p>The Schema Registry in {@code docker-compose.yml} is pinned to
 * {@code BACKWARD} compatibility. These tests exercise the exact same
 * compatibility engine offline (no running registry) so a contract change that
 * would break existing consumers fails the build instead of failing in prod.
 *
 * <p>Backward compatible = data written with the <i>old</i> schema can still be
 * read with the <i>new</i> schema. Adding an optional field is safe; changing
 * the wire type of an existing field number is not.
 */
class SchemaEvolutionTest {

    private static final String BASE = """
        syntax = "proto3";
        package rail.telemetry.v1;
        message PositionEvent {
          string event_id = 1;
          string trip_id = 2;
          double delay_seconds = 10;
        }
        """;

    @Test
    void addingAnOptionalFieldIsBackwardCompatible() {
        ProtobufSchema previous = new ProtobufSchema(BASE);
        ProtobufSchema evolved = new ProtobufSchema("""
            syntax = "proto3";
            package rail.telemetry.v1;
            message PositionEvent {
              string event_id = 1;
              string trip_id = 2;
              double delay_seconds = 10;
              string operator_id = 20; // NEW additive field
            }
            """);

        List<String> diffs = evolved.isBackwardCompatible(previous);
        assertThat(diffs)
                .as("adding an optional field must remain backward compatible")
                .isEmpty();
    }

    @Test
    void changingAnExistingFieldTypeBreaksCompatibility() {
        ProtobufSchema previous = new ProtobufSchema(BASE);
        ProtobufSchema broken = new ProtobufSchema("""
            syntax = "proto3";
            package rail.telemetry.v1;
            message PositionEvent {
              string event_id = 1;
              int64  trip_id = 2;       // BREAKING: string -> int64 on field #2
              double delay_seconds = 10;
            }
            """);

        List<String> diffs = broken.isBackwardCompatible(previous);
        assertThat(diffs)
                .as("changing the wire type of an existing field must be rejected")
                .isNotEmpty();
    }
}
