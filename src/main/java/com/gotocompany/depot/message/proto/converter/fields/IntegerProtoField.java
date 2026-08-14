package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * {@link ProtoField} strategy that converts all Protobuf integer field types into {@link Long} values.
 *
 * <p>It matches the full range of 32-bit and 64-bit integer types ({@code INT32}/{@code INT64} and
 * their unsigned, fixed, signed-fixed and zig-zag variants) and parses each value into a
 * {@link Long}. Repeated fields are converted element-wise into a list of longs.</p>
 *
 * @see ProtoFieldFactory
 */
public class IntegerProtoField implements ProtoField {
    /**
     * Descriptor of the field, used to confirm that it is one of the Protobuf integer types.
     */
    private final Descriptors.FieldDescriptor descriptor;
    /**
     * The raw field value: an integer value, or a collection of them for repeated fields.
     */
    private final Object fieldValue;

    /**
     * Creates a strategy for the given integer field.
     *
     * @param descriptor the descriptor of the field to convert
     * @param fieldValue the raw field value (an integer value, or a collection for repeated fields)
     */
    public IntegerProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    /**
     * Returns the field value converted to one or more {@link Long} values.
     *
     * <p>A repeated field is converted into a list with each element parsed individually, while a
     * singular field is converted directly.</p>
     *
     * @return the converted {@link Long}, or a list of {@link Long} for repeated fields
     */
    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getValue).collect(Collectors.toList());
        }
        return getValue(fieldValue);
    }

    /**
     * Parses a single value into a {@link Long}.
     *
     * @param field the value to parse; its {@code toString()} form is parsed as a long
     * @return the parsed long value
     * @throws NumberFormatException if the value cannot be parsed as a long
     */
    public Long getValue(Object field) {
        return Long.valueOf(field.toString());
    }

    /**
     * Indicates whether the field is one of the Protobuf integer types.
     *
     * @return {@code true} if the field's type is any 32-bit or 64-bit integer type, including the
     *     unsigned, fixed, signed-fixed and zig-zag variants, {@code false} otherwise
     */
    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.INT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.UINT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SFIXED64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SINT64
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.INT32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.UINT32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.FIXED32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SFIXED32
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.SINT32;
    }
}
