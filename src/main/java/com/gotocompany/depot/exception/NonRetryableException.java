package com.gotocompany.depot.exception;

/**
 * Signals that an operation has failed permanently and must not be retried.
 *
 * <p>Depot wraps certain failures in this exception to communicate unambiguously that further retry
 * attempts would be futile. It is raised by {@code RetryUtils} in two situations: when a caught
 * exception is rejected by the supplied retry predicate (classified as non-retryable), and when the
 * configured maximum number of retries has been exhausted without success. It is also thrown
 * directly by the MaxCompute insert manager when a record pack cannot be reconciled with the table
 * schema, since replaying the same data would fail identically.
 *
 * <p>This is an unchecked exception. When available, the originating failure is preserved as the
 * {@linkplain Throwable#getCause() cause}.
 */
public class NonRetryableException extends RuntimeException {

    /**
     * Creates a {@code NonRetryableException} with the supplied detail message and underlying cause.
     *
     * @param message human-readable description of the terminal failure
     * @param cause   the originating throwable that triggered or accumulated into this terminal
     *                failure; may be {@code null} when no underlying cause is available
     */
    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }

}
