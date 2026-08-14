package com.gotocompany.depot.schema;

/**
 * Enumerates the value categories a {@link SchemaField} can take in Depot's format-agnostic schema.
 *
 * <p>These constants form a small common type system onto which the supported payload formats are
 * mapped. For Protobuf the value is derived from the field's
 * {@link com.google.protobuf.Descriptors.FieldDescriptor.JavaType}, and for JSON it is inferred from
 * the runtime type of the parsed value. Sink-specific code switches on these constants to choose the
 * appropriate destination column type.</p>
 *
 * @see SchemaField#getType()
 */
public enum SchemaFieldType {
    /**
     * A 32-bit signed integer value.
     */
    INT,
    /**
     * A textual value, also used as the fallback type when a more specific type cannot be inferred.
     */
    STRING,
    /**
     * A single-precision floating-point value.
     */
    FLOAT,
    /**
     * A double-precision floating-point value.
     */
    DOUBLE,
    /**
     * A 64-bit signed integer value.
     */
    LONG,
    /**
     * A sequence of raw bytes.
     */
    BYTES,
    /**
     * An enumeration value.
     */
    ENUM,
    /**
     * A nested message (structured value) that has its own {@link Schema}.
     */
    MESSAGE,
    /**
     * A boolean value.
     */
    BOOLEAN;
}
