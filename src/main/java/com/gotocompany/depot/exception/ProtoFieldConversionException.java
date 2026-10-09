package com.gotocompany.depot.exception;

import lombok.Getter;

/**
 * Thrown when a proto field cannot be converted. Carries the full field path
 * (for example "cart.items.price") so the failing field can be located quickly.
 */
@Getter
public class ProtoFieldConversionException extends RuntimeException {
    private static final String PATH_SEPARATOR = ".";

    private final String fieldPath;
    private final String location;
    private final String rootMessage;

    private ProtoFieldConversionException(String fieldPath, String location, String rootMessage, Throwable cause) {
        super(String.format("Failed to convert field '%s' (%s): %s", fieldPath, location, rootMessage), cause);
        this.fieldPath = fieldPath;
        this.location = location;
        this.rootMessage = rootMessage;
    }

    /**
     * Wraps a failure raised while converting a field. If the cause is already a
     * ProtoFieldConversionException, the field name is prepended to its path, so the
     * path grows from the failing leaf up to the root.
     *
     * @param fieldName name of the field being converted at this level
     * @param indexInParent position of the field inside its parent message
     * @param level nesting level of the parent message
     * @param cause the failure
     * @return exception carrying the full path
     */
    public static ProtoFieldConversionException wrap(String fieldName, int indexInParent, int level, Throwable cause) {
        if (cause instanceof ProtoFieldConversionException) {
            ProtoFieldConversionException inner = (ProtoFieldConversionException) cause;
            return new ProtoFieldConversionException(fieldName + PATH_SEPARATOR + inner.fieldPath,
                    inner.location, inner.rootMessage, inner.getCause());
        }
        String message = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        String location = String.format("index %d in parent at level %d", indexInParent, level);
        return new ProtoFieldConversionException(fieldName, location, message, cause);
    }

    /**
     * Returns a copy that also names the message (position in batch, metadata such as the offset).
     *
     * @param messageContext description of the message being processed
     * @return exception with message context added
     */
    public ProtoFieldConversionException withMessageContext(String messageContext) {
        return new ProtoFieldConversionException(fieldPath, location + ", " + messageContext, rootMessage, getCause());
    }
}
