package com.gotocompany.depot.schema;

import java.util.List;

/**
 * Format-agnostic description of the structure of a parsed message.
 *
 * <p>{@code Schema} is Depot's common abstraction over the type systems of the payload formats it
 * supports. It is the schema counterpart of {@link com.gotocompany.depot.message.ParsedMessage} and
 * lets sink code (BigQuery, MaxCompute and others) inspect a record's shape, enumerate and look up
 * its fields, and detect well-known logical types without depending on Protobuf descriptors or JSON
 * structures directly. Implementations adapt a concrete schema source, such as a Protobuf
 * {@link com.google.protobuf.Descriptors.Descriptor} or a schema inferred from a JSON document.</p>
 *
 * @see SchemaField
 * @see LogicalType
 * @see com.gotocompany.depot.message.ParsedMessage#getSchema()
 */
public interface Schema {
    /**
     * Returns the fully qualified name of the type described by this schema.
     *
     * @return the schema's fully qualified name
     */
    String getFullName();

    /**
     * Returns the fields declared by this schema.
     *
     * @return the list of {@link SchemaField}s belonging to this schema
     */
    List<SchemaField> getFields();

    /**
     * Returns the field declared under the given name.
     *
     * <p>How an unknown name is handled is implementation-defined: an implementation may return
     * {@code null} or raise an unchecked exception.</p>
     *
     * @param name the name of the field to look up
     * @return the {@link SchemaField} declared under {@code name}
     */
    SchemaField getFieldByName(String name);

    /**
     * Returns the logical type of the value described by this schema.
     *
     * <p>This distinguishes ordinary messages from well-known types such as timestamps, durations,
     * structs and maps, allowing callers to apply special handling where required.</p>
     *
     * @return the {@link LogicalType} of this schema
     */
    LogicalType logicalType();
}
