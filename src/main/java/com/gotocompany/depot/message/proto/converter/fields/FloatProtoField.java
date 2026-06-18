package com.gotocompany.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * {@link ProtoField} strategy that converts Protobuf {@code FLOAT} and {@code DOUBLE} fields into
 * {@link Double} values.
 *
 * <p>Each value is parsed as a double and validated to be finite (neither infinite nor NaN). Repeated
 * fields are converted element-wise into a list of doubles.</p>
 *
 * @see ProtoFieldFactory
 */
public class FloatProtoField implements ProtoField {
    /**
     * The raw field value: a numeric value, or a collection of them for repeated fields.
     */
    private final Object fieldValue;
    /**
     * Descriptor of the field, used to confirm that it is a {@code FLOAT} or {@code DOUBLE} field.
     */
    private final Descriptors.FieldDescriptor descriptor;

    /**
     * Creates a strategy for the given float or double field.
     *
     * @param descriptor the descriptor of the field to convert
     * @param fieldValue the raw field value (a numeric value, or a collection for repeated fields)
     */
    public FloatProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    /**
     * Returns the field value converted to one or more {@link Double} values.
     *
     * <p>A repeated field is converted into a list with each element parsed and validated
     * individually, while a singular field is converted directly.</p>
     *
     * @return the converted {@link Double}, or a list of {@link Double} for repeated fields
     */
    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getValue).collect(Collectors.toList());
        }
        return getValue(fieldValue);
    }

    /**
     * Parses and validates a single numeric value as a finite {@link Double}.
     *
     * @param field the value to parse; its {@code toString()} form is parsed as a double
     * @return the parsed double value
     * @throws IllegalArgumentException if the parsed value is infinite or NaN
     * @throws NumberFormatException if the value cannot be parsed as a double
     */
    public Double getValue(Object field) {
        double val = Double.parseDouble(field.toString());
        boolean valid = !Double.isInfinite(val) && !Double.isNaN(val);
        if (!valid) {
            throw new IllegalArgumentException("Float/double value is not valid");
        }
        return val;
    }

    /**
     * Indicates whether the field is a Protobuf {@code FLOAT} or {@code DOUBLE} field.
     *
     * @return {@code true} if the field's type is {@code FLOAT} or {@code DOUBLE}, {@code false}
     *     otherwise
     */
    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.FLOAT
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.DOUBLE;
    }
}
