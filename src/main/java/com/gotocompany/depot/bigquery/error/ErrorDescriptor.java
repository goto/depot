package com.gotocompany.depot.bigquery.error;


/**
 * Descriptor interface that defines the various error descriptors and the corresponding error types.
 *
 * <p>Each concrete implementation inspects the {@code reason} and {@code message}
 * text that BigQuery reports for a failed row (see
 * {@link com.google.cloud.bigquery.BigQueryError}) and decides, through
 * {@link #matches()}, whether that error belongs to the specific failure category it
 * represents (for example an invalid schema, an out-of-bounds partition value, or a
 * stopped insertion). Implementations also expose a human readable
 * {@link Object#toString()} describing the error.</p>
 *
 * <p>Descriptors are evaluated by {@link ErrorParser}, which returns the first
 * descriptor whose {@link #matches()} method returns {@code true} and falls back to
 * {@link UnknownError} when none match.</p>
 *
 * @see ErrorParser
 */
public interface ErrorDescriptor {

    /**
     * If the implementing descriptor matches the condition as prescribed in the concrete implementation.
     *
     * <p>The matching condition is defined by each concrete implementation, typically
     * by comparing the BigQuery error reason and message against a known signature.</p>
     *
     * @return - true if the condition matches, false otherwise.
     */
    boolean matches();
}
