package com.rail.platform.ingestion.domain.exception;

/**
 * Raised when a raw telemetry payload cannot be normalized into the platform's
 * published language (missing mandatory identity, unparseable structure, etc.).
 *
 * <p>This is a <em>domain</em> exception: it expresses a business rule of the
 * ingestion context ("a telemetry signal without a journey identity is not
 * admissible"), independent of any framework. The application service catches
 * it and routes the original bytes to the dead-letter queue rather than
 * dropping the frame — satisfying the "zero message handling exceptions" clause
 * of the DoD.
 */
public class PayloadNormalizationException extends RuntimeException {

    public PayloadNormalizationException(String message) {
        super(message);
    }

    public PayloadNormalizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
