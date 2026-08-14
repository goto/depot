package com.gotocompany.depot.exception;

/**
 * Signals that a message field holds a value that is structurally parseable but semantically invalid
 * for the target sink.
 *
 * <p>Whereas {@link DeserializerException} indicates that a payload could not be decoded at all, this
 * exception is raised after decoding, when an individual value violates a domain rule enforced by a
 * sink. Within Depot it is thrown, for example, by the MaxCompute numeric mappers when a
 * {@code float} or {@code double} value is not finite (such as {@code NaN} or infinity) and cannot be
 * represented in the destination column, and by timestamp validation when an event time falls
 * outside the configured valid range or the permitted past/future window.
 *
 * <p>This is an unchecked exception. Because it reflects a property of the data itself, retrying the
 * same record will not help; such records are normally routed to error handling.
 */
public class InvalidMessageException extends RuntimeException {
    /**
     * Creates an {@code InvalidMessageException} with the supplied detail message.
     *
     * @param message human-readable description of why the value was rejected, typically including
     *                the offending value or the violated constraint
     */
    public InvalidMessageException(String message) {
        super(message);
    }
}
