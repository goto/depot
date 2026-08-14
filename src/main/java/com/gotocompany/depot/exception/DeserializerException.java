package com.gotocompany.depot.exception;

/**
 * Deserializer exception is thrown when message from proto is not deserializable into the Java object.
 *
 * <p>This exception is the base of Depot's deserialization-failure hierarchy and is raised whenever a
 * Protobuf or JSON payload cannot be decoded into the expected Java object: for example when the
 * bytes do not conform to the active schema, when a logical type such as a timestamp, struct, or
 * duration cannot be converted, or when a message cannot be rendered as JSON. More specific
 * conditions are represented by its subclasses {@link EmptyMessageException} (empty payloads) and
 * {@link UnknownFieldsException} (payloads carrying fields absent from the schema).
 *
 * <p>This is an unchecked exception. It generally reflects a mismatch between the data and the active
 * schema; whether it is recoverable depends on the cause, since an out-of-date schema may be fixed by
 * a schema refresh whereas genuinely corrupt input cannot.
 */
public class DeserializerException extends RuntimeException {

    /**
     * Creates a {@code DeserializerException} with the supplied detail message and no cause.
     *
     * @param message human-readable description of the deserialization failure
     */
    public DeserializerException(String message) {
        super(message);
    }

    /**
     * Creates a {@code DeserializerException} with the supplied detail message and originating cause.
     *
     * @param message human-readable description of the deserialization failure
     * @param e       the underlying exception that caused deserialization to fail
     */
    public DeserializerException(String message, Exception e) {
        super(message, e);
    }
}
