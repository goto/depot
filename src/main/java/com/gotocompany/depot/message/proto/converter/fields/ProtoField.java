package com.gotocompany.depot.message.proto.converter.fields;

/**
 * Strategy that converts the value of a single Protobuf field into a representation suitable for
 * downstream serialization, most notably the BigQuery storage write path.
 *
 * <p>Each implementation targets a specific Protobuf field category (integer, float, byte string,
 * enum, timestamp, duration, struct, map or generic message) and declares, through {@link #matches()},
 * whether it applies to a given field descriptor. {@link ProtoFieldFactory} consults the available
 * implementations in order and selects the first whose {@link #matches()} returns {@code true},
 * falling back to {@link DefaultProtoField} when none match. The chosen implementation then exposes
 * the converted value via {@link #getValue()}, transparently handling both singular and repeated
 * (collection) field values.</p>
 *
 * @see ProtoFieldFactory
 * @see DefaultProtoField
 */
public interface ProtoField {

    /**
     * Returns the converted value of the underlying Protobuf field.
     *
     * <p>For a repeated field the result is typically a list of converted elements; for a singular
     * field it is the converted scalar or message value.</p>
     *
     * @return the converted field value
     */
    Object getValue();

    /**
     * Indicates whether this strategy applies to the underlying field.
     *
     * @return {@code true} if this implementation should be used to convert the field, {@code false}
     *     otherwise
     */
    boolean matches();
}
