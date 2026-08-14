package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;

/**
 * {@link SchemaField} implementation backed by a Protobuf {@link Descriptors.FieldDescriptor}.
 *
 * <p>This adapter exposes a single Protobuf field through Depot's format-agnostic schema abstraction,
 * translating the descriptor's Java type into a {@link SchemaFieldType} and surfacing nested message
 * types as further {@link ProtoSchema} instances.</p>
 *
 * @see SchemaField
 * @see ProtoSchema
 */
public class ProtoSchemaField implements SchemaField {
    /**
     * Protobuf descriptor of the field this instance describes.
     */
    private Descriptors.FieldDescriptor fd;

    /**
     * Creates a schema-field view over the given Protobuf field descriptor.
     *
     * @param fieldDescriptor the descriptor of the Protobuf field to describe
     */
    public ProtoSchemaField(Descriptors.FieldDescriptor fieldDescriptor) {
        this.fd = fieldDescriptor;
    }

    /**
     * Returns the declared (proto) name of the field.
     *
     * @return the field's name
     */
    @Override
    public String getName() {
        return fd.getName();
    }

    /**
     * Returns the JSON name of the field as derived by Protobuf.
     *
     * @return the field's JSON name, typically the camel-cased field name
     */
    @Override
    public String getJsonName() {
        return fd.getJsonName();
    }

    /**
     * Returns the type of the field.
     *
     * <p>Maps the descriptor's {@link Descriptors.FieldDescriptor.JavaType} onto a
     * {@link SchemaFieldType}: a {@code BYTE_STRING} becomes {@link SchemaFieldType#BYTES}, while every
     * other Java type maps to the {@link SchemaFieldType} constant of the same name.</p>
     *
     * @return the field's type
     */
    @Override
    public SchemaFieldType getType() {
        switch (fd.getJavaType()) {
            case BYTE_STRING:
                return SchemaFieldType.BYTES;
            default:
                return SchemaFieldType.valueOf(fd.getJavaType().name());
        }
    }

    /**
     * Returns the schema of the nested message when this field is a message, or {@code null}
     * otherwise.
     *
     * <p>A {@link ProtoSchema} describing the nested message type is returned when this field's Java
     * type is {@code MESSAGE}; for all other field types {@code null} is returned.</p>
     *
     * @return the nested message schema, or {@code null} if the field is not a message
     */
    @Override
    public Schema getValueType() {
        if (fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE)) {
            return new ProtoSchema(fd.getMessageType());
        }
        return null;
    }

    /**
     * Reports whether the underlying field is repeated.
     *
     * @return {@code true} if the field is repeated, {@code false} otherwise
     */
    @Override
    public boolean isRepeated() {
        return fd.isRepeated();
    }
}
