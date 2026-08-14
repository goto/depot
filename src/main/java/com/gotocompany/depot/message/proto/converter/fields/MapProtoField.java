package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

/**
 * {@link ProtoField} strategy for Protobuf map fields that returns the map value unchanged.
 *
 * <p>It matches any field declared as a Protobuf {@code map}, which Protobuf models as a repeated
 * message of key/value entries, and exposes the value as-is. Note that this strategy is not part of
 * the default ordering used by {@link ProtoFieldFactory}; it is available for callers that need to
 * recognise and pass through map fields explicitly.</p>
 *
 * @see ProtoFieldFactory
 */
public class MapProtoField implements ProtoField {

    /**
     * Descriptor of the field, used to confirm that it is a map field.
     */
    private final Descriptors.FieldDescriptor descriptor;
    /**
     * The raw map value, returned unchanged by {@link #getValue()}.
     */
    private final Object fieldValue;

    /**
     * Creates a strategy for the given map field.
     *
     * @param descriptor the descriptor of the field to convert
     * @param fieldValue the raw map field value
     */
    public MapProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    /**
     * Returns the map field value unchanged.
     *
     * @return the raw field value supplied at construction
     */
    @Override
    public Object getValue() {
        return fieldValue;
    }

    /**
     * Indicates whether the field is a Protobuf map field.
     *
     * @return {@code true} if the descriptor reports a map field, {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return descriptor.isMapField();
    }
}
