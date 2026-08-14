package com.gotocompany.depot.exception;

/**
 * Empty thrown when the message is contains zero bytes.
 *
 * <p>Depot deserializes each incoming record from its raw {@code byte[]} payload, taking either the
 * log message or the log key depending on the configured schema message mode. When that payload is
 * {@code null} or empty there is nothing to parse, so the parser short-circuits by raising this
 * exception instead of attempting deserialization. Both the Protobuf and JSON message parsers throw
 * it when handed an empty payload.
 *
 * <p>This is a specialization of {@link DeserializerException} and is therefore an unchecked
 * exception. It typically indicates tombstone-like or malformed input rather than a transient
 * failure, so re-processing the same empty payload will not succeed.
 */
public class EmptyMessageException extends DeserializerException {
    /**
     * Creates an {@code EmptyMessageException} with the fixed detail message {@code "log message is empty"}.
     *
     * <p>The exception carries no cause; the empty payload itself is the failure condition.
     */
    public EmptyMessageException() {
        super("log message is empty");
    }
}

