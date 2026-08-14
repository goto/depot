package com.gotocompany.depot.bigquery.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.DescriptorProtos;
import com.gotocompany.depot.bigquery.exception.BQSchemaMappingException;
import com.gotocompany.depot.message.proto.Constants;
import com.gotocompany.depot.message.proto.ProtoField;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps a protobuf field definition to its BigQuery {@link Field} representation.
 *
 * <p>A {@code BQField} captures the BigQuery column name, mode ({@link Field.Mode}) and
 * type ({@link LegacySQLTypeName}) derived from a {@link ProtoField}, together with any
 * nested sub-fields for record (message) types. It is the building block used by the
 * schema generator to translate a protobuf schema into a BigQuery schema.</p>
 *
 * <p>The translation relies on three static lookup tables that map protobuf labels to
 * BigQuery modes, protobuf field types to BigQuery types, and certain well-known protobuf
 * type names (such as {@code Timestamp}, {@code Struct} and {@code Duration}) to specific
 * BigQuery types.</p>
 *
 * @see com.gotocompany.depot.bigquery.proto.BigqueryFields
 */
@EqualsAndHashCode
public class BQField {
    /** Maps protobuf field labels (optional, repeated, required) to BigQuery field modes. */
    private static final Map<DescriptorProtos.FieldDescriptorProto.Label, Field.Mode> FIELD_LABEL_TO_BQ_MODE_MAP = new HashMap<DescriptorProtos.FieldDescriptorProto.Label, Field.Mode>() {{
        put(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, Field.Mode.NULLABLE);
        put(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED, Field.Mode.REPEATED);
        put(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED, Field.Mode.REQUIRED);
    }};
    /** Maps protobuf field types to their corresponding BigQuery types. */
    private static final Map<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName> FIELD_TYPE_TO_BQ_TYPE_MAP = new HashMap<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName>() {{
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, LegacySQLTypeName.BYTES);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, LegacySQLTypeName.BOOLEAN);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64, LegacySQLTypeName.INTEGER);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE, LegacySQLTypeName.RECORD);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_GROUP, LegacySQLTypeName.RECORD);
    }};
    /** Maps well-known protobuf type names (timestamp, struct, duration) to BigQuery types. */
    private static final Map<String, LegacySQLTypeName> FIELD_NAME_TO_BQ_TYPE_MAP = new HashMap<String, LegacySQLTypeName>() {{
        put(Constants.ProtobufTypeName.TIMESTAMP_PROTOBUF_TYPE_NAME, LegacySQLTypeName.TIMESTAMP);
        put(Constants.ProtobufTypeName.STRUCT_PROTOBUF_TYPE_NAME, LegacySQLTypeName.STRING);
        put(Constants.ProtobufTypeName.DURATION_PROTOBUF_TYPE_NAME, LegacySQLTypeName.RECORD);
    }};
    /** The BigQuery column name. */
    private final String name;
    /** The BigQuery field mode (for example nullable, repeated or required). */
    private final Field.Mode mode;
    /** The BigQuery column type. */
    private final LegacySQLTypeName type;
    /** Nested sub-fields for record types; empty for leaf fields. */
    private List<Field> subFields;

    /**
     * Creates a field directly from its BigQuery attributes.
     *
     * @param name      the BigQuery column name
     * @param mode      the BigQuery field mode (for example {@code NULLABLE},
     *                  {@code REPEATED} or {@code REQUIRED})
     * @param type      the BigQuery column type
     * @param subFields the nested sub-fields for record types, or an empty list when the
     *                  field is a leaf
     */
    public BQField(String name, Field.Mode mode, LegacySQLTypeName type, List<Field> subFields) {
        this.name = name;
        this.mode = mode;
        this.type = type;
        this.subFields = subFields;
    }

    /**
     * Creates a field by translating a protobuf field definition.
     *
     * <p>Derives the BigQuery mode from the protobuf label and the BigQuery type from the
     * protobuf type or well-known type name; sub-fields start empty and may be populated
     * later via {@link #setSubFields(List)}.</p>
     *
     * @param protoField the protobuf field to translate
     * @throws BQSchemaMappingException if the protobuf field has no corresponding BigQuery
     *                                  type mapping
     */
    public BQField(ProtoField protoField) {
        this.name = protoField.getName();
        this.mode = FIELD_LABEL_TO_BQ_MODE_MAP.get(protoField.getLabel());
        this.type = getType(protoField);
        this.subFields = new ArrayList<>();
    }

    /**
     * Map fully qualified type name or protobuf type to bigquery types.
     * Fully qualified name will be used as mapping key before protobuf type being used
     *
     * <p>Maps the field's fully qualified protobuf type name to a BigQuery type when one is
     * known, taking precedence over the mapping derived from the raw protobuf field type.
     * This lets well-known types such as {@code Timestamp} map to dedicated BigQuery types
     * instead of their structural representation.</p>
     *
     * @param protoField
     * @return
     * @throws BQSchemaMappingException if neither the type name nor the protobuf type has a
     *                                  known BigQuery mapping
     */
    private LegacySQLTypeName getType(ProtoField protoField) {
        LegacySQLTypeName typeFromFieldName = FIELD_NAME_TO_BQ_TYPE_MAP.get(protoField.getTypeName()) != null
                ? FIELD_NAME_TO_BQ_TYPE_MAP.get(protoField.getTypeName())
                : FIELD_TYPE_TO_BQ_TYPE_MAP.get(protoField.getType());
        if (typeFromFieldName == null) {
            throw new BQSchemaMappingException(String.format("No type mapping found for field: %s, fieldType: %s, typeName: %s", protoField.getName(), protoField.getType(), protoField.getTypeName()));
        }
        return typeFromFieldName;
    }

    /**
     * Replaces this field's nested sub-fields.
     *
     * @param fields the nested BigQuery fields to associate with this record field
     */
    public void setSubFields(List<Field> fields) {
        this.subFields = fields;
    }

    /**
     * Builds the BigQuery {@link Field} represented by this instance.
     *
     * <p>When there are no sub-fields a leaf field of the configured name, type and mode is
     * produced; otherwise a record field wrapping the sub-fields is produced.</p>
     *
     * @return the BigQuery field, including any nested sub-fields
     */
    public Field getField() {
        if (this.subFields == null || this.subFields.size() == 0) {
            return Field.newBuilder(this.name, this.type).setMode(this.mode).build();
        }
        return Field.newBuilder(this.name, this.type, FieldList.of(subFields)).setMode(this.mode).build();
    }

    /**
     * Returns the BigQuery column name of this field.
     *
     * @return the field name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the BigQuery type of this field.
     *
     * @return the field's {@link LegacySQLTypeName}
     */
    public LegacySQLTypeName getType() {
        return type;
    }


}
