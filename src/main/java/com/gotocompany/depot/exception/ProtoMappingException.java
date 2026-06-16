package com.gotocompany.depot.exception;

/**
 * Exception thrown when a CEL mapping expression cannot be evaluated or its result cannot be assigned to a proto field.
 */
public class ProtoMappingException extends RuntimeException {

    /**
     * Creates an exception with the given message.
     *
     * @param message the detail message
     */
    public ProtoMappingException(String message) {
        super(message);
    }

    /**
     * Creates an exception with the given message and cause.
     *
     * @param message the detail message
     * @param cause   the underlying cause
     */
    public ProtoMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
