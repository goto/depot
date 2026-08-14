package com.gotocompany.depot.exception;

/**
 * Signals that Depot has been given invalid, incomplete, or unsupported configuration.
 *
 * <p>This exception is raised when a component cannot proceed because of how it was configured, rather
 * than because of the data it is processing. It is used broadly across Depot, for example when an
 * unsupported schema type is selected, when a Redis connection URL or a MaxCompute {@code ZoneId}
 * cannot be parsed, when a positive Redis TTL value is required but missing, when a Bigtable
 * column-family mapping is empty, when templates are used in an unsupported request mode, or when a
 * sink fails to be constructed.
 *
 * <p>This is an unchecked exception and normally indicates a deployment or setup mistake that must be
 * corrected in configuration; it is not resolved by retrying.
 */
public class ConfigurationException extends RuntimeException {
    /**
     * Creates a {@code ConfigurationException} with the supplied detail message and no cause.
     *
     * @param message human-readable description of the configuration problem
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a {@code ConfigurationException} with the supplied detail message and originating cause.
     *
     * @param message human-readable description of the configuration problem
     * @param cause   the underlying throwable that surfaced the configuration problem
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
