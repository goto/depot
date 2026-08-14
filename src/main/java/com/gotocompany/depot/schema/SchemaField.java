package com.gotocompany.depot.schema;

/**
 * Format-agnostic description of a single field within a {@link Schema}.
 *
 * <p>A {@code SchemaField} exposes the attributes of one field that Depot's sinks need in order to map
 * it onto a destination column: its name, its JSON name, its {@link SchemaFieldType}, the nested
 * {@link Schema} of its value when the field is itself a message, and whether the field is repeated.
 * Implementations adapt a concrete field representation such as a Protobuf
 * {@link com.google.protobuf.Descriptors.FieldDescriptor} or a field inferred from a JSON value.</p>
 *
 * @see Schema
 * @see SchemaFieldType
 */
public interface SchemaField {
    /**
     * Returns the declared name of the field.
     *
     * @return the field's name as declared in its source schema
     */
    String getName();

    /**
     * Returns the JSON name of the field.
     *
     * <p>This is the name used when the field is represented in JSON, which may differ from
     * {@link #getName()}, for example a Protobuf field's camel-cased JSON name.</p>
     *
     * @return the field's JSON name
     */
    String getJsonName();

    /**
     * Returns the type of the field.
     *
     * @return the {@link SchemaFieldType} categorizing this field's value
     */
    SchemaFieldType getType();

    /**
     * Returns the schema of this field's value when the field is a nested message.
     *
     * <p>For fields whose {@link #getType()} is {@link SchemaFieldType#MESSAGE} this describes the
     * structure of the nested value. The result for non-message fields is implementation-defined.</p>
     *
     * @return the nested {@link Schema} of the field's value, where applicable
     */
    Schema getValueType();

    /**
     * Reports whether the field holds a repeated (list-valued) value.
     *
     * @return {@code true} if the field is repeated, {@code false} otherwise
     */
    boolean isRepeated();
}
