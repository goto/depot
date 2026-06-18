package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link Schema} implementation backed by a Protobuf {@link Descriptors.Descriptor}.
 *
 * <p>This adapter exposes a Protobuf message type through Depot's format-agnostic schema abstraction.
 * Field metadata is derived lazily from the descriptor on first access and cached, and well-known
 * Protobuf types are recognized and reported through {@link #logicalType()}.</p>
 *
 * <p>The lazy field cache is populated without synchronization, so an instance should not be shared
 * across threads while its fields are first being initialized.</p>
 *
 * @see Schema
 * @see ProtoSchemaField
 */
public class ProtoSchema implements Schema {
    /**
     * Protobuf descriptor describing the message type this schema represents.
     */
    private final Descriptors.Descriptor descriptor;

    /**
     * Lazily initialized cache mapping each field name to its {@link ProtoSchemaField}; {@code null}
     * until {@link #initializeFields()} runs.
     */
    private Map<String, ProtoSchemaField> fields;

    /**
     * Creates a schema view over the given Protobuf descriptor.
     *
     * @param descriptor the descriptor of the Protobuf message type to describe
     */
    public ProtoSchema(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * Returns the fully qualified name of the underlying Protobuf message type.
     *
     * @return the descriptor's fully qualified name
     */
    @Override
    public String getFullName() {
        return descriptor.getFullName();
    }

    /**
     * Returns the fields of the underlying Protobuf message type.
     *
     * <p>The field cache is initialized on first use, and a new list containing the cached fields is
     * returned on each call.</p>
     *
     * @return the list of fields declared by the message type
     */
    @Override
    public List<SchemaField> getFields() {
        initializeFields();
        return new ArrayList<>(fields.values());
    }

    /**
     * Returns the field declared under the given name, or {@code null} when it does not exist.
     *
     * <p>The field cache is initialized on first use and the name is then looked up within it.</p>
     *
     * @param name the name of the field to look up
     * @return the matching {@link ProtoSchemaField}, or {@code null} if the message type has no field
     *     with that name
     */
    @Override
    public SchemaField getFieldByName(String name) {
        initializeFields();
        return fields.get(name);
    }

    /**
     * Returns the logical type of the underlying message type.
     *
     * <p>Recognizes well-known Protobuf types by fully qualified name and map entries by their
     * descriptor options: {@code google.protobuf.Timestamp} maps to {@link LogicalType#TIMESTAMP},
     * {@code google.protobuf.Duration} to {@link LogicalType#DURATION}, {@code google.protobuf.Struct}
     * to {@link LogicalType#STRUCT}, and a map-entry message to {@link LogicalType#MAP}. Any other
     * message type maps to {@link LogicalType#MESSAGE}.</p>
     *
     * @return the logical type derived from the descriptor
     */
    @Override
    public LogicalType logicalType() {
        String fullName = descriptor.getFullName();
        if (Timestamp.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.TIMESTAMP;
        } else if (Duration.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.DURATION;
        } else if (Struct.getDescriptor().getFullName().equals(fullName)) {
            return LogicalType.STRUCT;
        } else if (descriptor.getOptions().getMapEntry()) {
            return LogicalType.MAP;
        }
        return LogicalType.MESSAGE;
    }

    /**
     * Lazily populates the field cache from the descriptor.
     *
     * <p>On the first invocation each field of the descriptor is wrapped in a {@link ProtoSchemaField}
     * and collected into a map keyed by field name; later invocations are no-ops. The initialization
     * is not synchronized.</p>
     */
    private void initializeFields() {
        if (fields == null) {
            fields = descriptor.getFields().stream()
                    .map(ProtoSchemaField::new)
                    .collect(Collectors.toMap(ProtoSchemaField::getName, Function.identity()));
        }
    }
}
