package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

/**
 * {@link ProtoField} strategy that recognises {@code google.protobuf.Duration} fields and returns the
 * value unchanged.
 *
 * <p>It matches any message field whose type is {@code google.protobuf.Duration} and exposes the raw
 * duration message as-is, leaving any further interpretation to the caller. It is listed first in
 * {@link ProtoFieldFactory} so that duration fields are not captured by the generic message
 * strategy.</p>
 *
 * @see ProtoFieldFactory
 */
public class DurationProtoField implements ProtoField {
    /**
     * Descriptor of the field, used to confirm that it is a {@code google.protobuf.Duration} field.
     */
    private final Descriptors.FieldDescriptor descriptor;
    /**
     * The raw duration value, returned unchanged by {@link #getValue()}.
     */
    private final Object fieldValue;

    /**
     * Creates a strategy for the given duration field.
     *
     * @param descriptor the descriptor of the field to convert
     * @param fieldValue the raw duration field value
     */
    public DurationProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    /**
     * Returns the duration field value unchanged.
     *
     * @return the raw field value supplied at construction
     */
    @Override
    public Object getValue() {
        return fieldValue;
    }

    /**
     * Indicates whether the field is a {@code google.protobuf.Duration} message field.
     *
     * @return {@code true} if the field is a message whose type is {@code google.protobuf.Duration},
     *     {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && descriptor.getMessageType().getFullName().equals(com.google.protobuf.Duration.getDescriptor().getFullName());
    }
}
