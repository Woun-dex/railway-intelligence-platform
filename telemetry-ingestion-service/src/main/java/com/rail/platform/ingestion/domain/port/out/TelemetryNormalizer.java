package com.rail.platform.ingestion.domain.port.out;

import com.rail.platform.ingestion.domain.model.NormalizedTelemetry;
import com.rail.platform.ingestion.domain.model.RawTelemetry;

/**
 * Outbound port: the Anti-Corruption Layer contract.
 *
 * <p>Upstream feeds (SIRI-ET, GTFS-RT) speak their own foreign languages —
 * different casings, field names, time and duration formats. This port defines
 * the translation the context requires: turn a {@link RawTelemetry} frame into
 * a {@link NormalizedTelemetry} expressed in the platform's published language.
 * The implementation (an infrastructure adapter) absorbs all the messiness of
 * the foreign formats so the rest of the domain never sees them.
 *
 * <p>It is synchronous and pure (no I/O): a contract violation surfaces as a
 * {@code PayloadNormalizationException}, which the application service turns
 * into a dead letter.
 */
public interface TelemetryNormalizer {

    NormalizedTelemetry normalize(RawTelemetry raw);
}
