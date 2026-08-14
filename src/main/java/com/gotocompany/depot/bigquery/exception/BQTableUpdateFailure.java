package com.gotocompany.depot.bigquery.exception;

/**
 * Unchecked exception thrown when updating or upserting a BigQuery table fails.
 *
 * <p>Raised by the schema update listeners when applying a schema change to the
 * destination table fails, wrapping the underlying cause (such as a
 * {@link com.google.cloud.bigquery.BigQueryException} or a
 * {@link java.io.IOException}).</p>
 */
public class BQTableUpdateFailure extends RuntimeException {
    /**
     * Creates the exception with a message and the underlying cause of the failure.
     *
     * @param message   the detail message describing the failed table update
     * @param rootCause the underlying throwable that caused the update to fail
     */
    public BQTableUpdateFailure(String message, Throwable rootCause) {
        super(message, rootCause);
    }
}
