package com.gotocompany.depot.bigquery.proto;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.gotocompany.depot.bigquery.models.BQField;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.message.proto.ProtoField;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility helpers that build BigQuery {@link Field} definitions from configuration and protobuf schemas.
 *
 * <p>Provides factory methods for the metadata columns added to every table (flat or
 * namespaced) and for translating a {@link ProtoField} tree into the list of BigQuery
 * fields that make up a table schema. All methods are {@code static}; the class is not
 * intended to be instantiated.</p>
 *
 * @see com.gotocompany.depot.bigquery.models.BQField
 */
public class BigqueryFields {
    /**
     * Builds the flat list of BigQuery metadata fields from their configured types.
     *
     * <p>Each tuple's value is resolved with {@link LegacySQLTypeName#valueOf(String)} and
     * the resulting field is marked {@code NULLABLE}.</p>
     *
     * @param metadataColumnsTypes the configured metadata columns, each a tuple of a name
     *                             and a BigQuery type name
     * @return the list of nullable BigQuery metadata {@link Field} definitions
     */
    public static List<Field> getMetadataFields(List<TupleString> metadataColumnsTypes) {
        return metadataColumnsTypes.stream().map(
                tuple -> Field.newBuilder(tuple.getFirst(), LegacySQLTypeName.valueOf(tuple.getSecond()))
                        .setMode(Field.Mode.NULLABLE)
                        .build()).collect(Collectors.toList());
    }

    /*
    throws an exception if typeName is not recognized by LegacySQLTypeName.valueOfStric
     */
    /**
     * Builds the metadata fields, strictly validating each configured type name.
     *
     * <p>Like {@link #getMetadataFields(List)} but resolves each type via
     * {@link LegacySQLTypeName#valueOfStrict(String)} (upper-cased), so an unrecognised
     * type name is rejected rather than silently accepted. Each field is marked
     * {@code NULLABLE}.</p>
     *
     * @param metadataColumnsTypes the configured metadata columns, each a tuple of a name
     *                             and a BigQuery type name
     * @return the list of nullable BigQuery metadata {@link Field} definitions
     * @throws IllegalArgumentException if a configured type name is not a recognised
     *                                  {@link LegacySQLTypeName}
     */
    public static List<Field> getMetadataFieldsStrict(List<TupleString> metadataColumnsTypes) {
        return metadataColumnsTypes.stream().map(
                tuple -> Field.newBuilder(tuple.getFirst(), LegacySQLTypeName.valueOfStrict(tuple.getSecond().toUpperCase()))
                        .setMode(Field.Mode.NULLABLE)
                        .build()).collect(Collectors.toList());
    }

    /**
     * Builds a single record field that nests all metadata columns under a namespace.
     *
     * <p>The metadata columns (resolved via {@link #getMetadataFields(List)}) become the
     * sub-fields of a {@code RECORD} field named after the namespace; the record field is
     * marked {@code NULLABLE}.</p>
     *
     * @param namespace            the name of the enclosing record field
     * @param metadataColumnsTypes the configured metadata columns to nest, each a tuple of
     *                             a name and a BigQuery type name
     * @return a nullable BigQuery {@code RECORD} {@link Field} containing the metadata
     *         columns
     */
    public static Field getNamespacedMetadataField(String namespace, List<TupleString> metadataColumnsTypes) {
        return Field.newBuilder(namespace, LegacySQLTypeName.RECORD, FieldList.of(getMetadataFields(metadataColumnsTypes)))
                .setMode(Field.Mode.NULLABLE)
                .build();
    }

    /**
     * Translates a protobuf field tree into the corresponding BigQuery schema fields.
     *
     * <p>Iterates over the children of the supplied {@link ProtoField}, converting each to a
     * {@link BQField}. Nested message fields are translated recursively and attached as
     * sub-fields, so the full protobuf hierarchy is reflected in the resulting BigQuery
     * fields.</p>
     *
     * @param protoField the root protobuf field whose children describe the schema, or
     *                   {@code null}
     * @return the list of BigQuery {@link Field} definitions, or {@code null} when
     *         {@code protoField} is {@code null}
     * @throws com.gotocompany.depot.bigquery.exception.BQSchemaMappingException if a nested
     *                   protobuf field cannot be mapped to a BigQuery type
     */
    public static List<Field> generateBigquerySchema(ProtoField protoField) {
        if (protoField == null) {
            return null;
        }
        List<Field> schemaFields = new ArrayList<>();
        for (ProtoField field : protoField.getFields()) {
            BQField bqField = new BQField(field);
            if (field.isNested()) {
                List<Field> fields = generateBigquerySchema(field);
                bqField.setSubFields(fields);
            }
            schemaFields.add(bqField.getField());
        }
        return schemaFields;
    }
}
