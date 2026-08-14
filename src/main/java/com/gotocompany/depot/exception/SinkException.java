package com.gotocompany.depot.exception;

import java.io.IOException;

/**
 * Signals a failure that occurs while a sink pushes records to its destination.
 *
 * <p>This exception represents errors surfaced at the sink boundary, for instance an interruption
 * while writing through the BigQuery Storage API or a failure while executing an HTTP request. It
 * extends {@link java.io.IOException}, making it a checked exception that propagates I/O-style
 * failures from the write path to the caller.
 *
 * <p>The triggering error is preserved as the {@linkplain Throwable#getCause() cause} so callers can
 * inspect the root failure and decide how to respond.
 */
public class SinkException extends IOException {
    /**
     * Creates a {@code SinkException} with the supplied detail message and underlying cause.
     *
     * @param message human-readable description of the sink failure
     * @param th      the underlying throwable that caused the sink operation to fail
     */
    public SinkException(String message, Throwable th) {
        super(message, th);
    }
}
