package com.gotocompany.depot.bigquery.proto;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.DescriptorProtos;
import com.gotocompany.depot.message.proto.TestProtoUtil;
import com.gotocompany.depot.message.proto.Constants;
import com.gotocompany.depot.message.proto.ProtoField;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link BigqueryFields#generateBigquerySchema}, which converts a tree of
 * {@link ProtoField}s into a list of BigQuery {@link Field}s.
 *
 * <p>The tests build {@link ProtoField} trees with {@link TestProtoUtil} and assert that the produced
 * BigQuery fields have the expected names, modes ({@code NULLABLE} or {@code REPEATED}) and types.
 * This includes the mapping of every integer Protobuf type to {@code INTEGER}, recursive handling of
 * nested and multi-nested messages, and the special handling of the well-known timestamp, struct,
 * duration and date types.</p>
 */
public class BigqueryFieldsTest {

    /** Maps the simple Protobuf scalar types to their expected BigQuery {@link LegacySQLTypeName}. */
    private final Map<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName> expectedType = new HashMap<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName>() {{
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, LegacySQLTypeName.BYTES);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, LegacySQLTypeName.BOOLEAN);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, LegacySQLTypeName.FLOAT);
    }};

    /**
     * Verifies that a flat set of scalar fields converts to nullable BigQuery fields of matching type.
     *
     * <p>Given six scalar proto fields (bytes, string, bool, enum, double and float), when the schema
     * is generated, then each BigQuery field is {@code NULLABLE}, keeps its name and maps to the
     * expected type from {@code expectedType}.</p>
     */
    @Test
    public void shouldTestConvertToSchemaSuccessful() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(TestProtoUtil.createProtoField("field0_bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field1_string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field2_bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field3_enum", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field4_double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field5_float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));


        List<Field> fields = BigqueryFields.generateBigquerySchema(TestProtoUtil.createProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(expectedType.get(nestedBQFields.get(index).getType()), fields.get(index).getType());
                });
    }

    /**
     * Verifies that every Protobuf integer type maps to a nullable BigQuery {@code INTEGER}.
     *
     * <p>Given proto fields for all ten integer variants (int64/uint64/int32/uint32, the fixed and
     * sfixed forms and the sint forms), when the schema is generated, then each field is
     * {@code NULLABLE}, keeps its name and maps to {@link LegacySQLTypeName#INTEGER}.</p>
     */
    @Test
    public void shouldTestShouldConvertIntegerDataTypes() {
        List<DescriptorProtos.FieldDescriptorProto.Type> allIntTypes = new ArrayList<DescriptorProtos.FieldDescriptorProto.Type>() {{
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        }};

        List<ProtoField> nestedBQFields = IntStream.range(0, allIntTypes.size())
                .mapToObj(index -> TestProtoUtil.createProtoField("field-" + index, allIntTypes.get(index), DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .collect(Collectors.toList());


        List<Field> fields = BigqueryFields.generateBigquerySchema(TestProtoUtil.createProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(LegacySQLTypeName.INTEGER, fields.get(index).getType());
                });
    }


    /**
     * Verifies that a single level of message nesting becomes a BigQuery record with sub-fields.
     *
     * <p>Given a message field with two nested string fields alongside a top-level scalar, when the
     * schema is generated, then the scalar maps to a string field and the message maps to a
     * {@link LegacySQLTypeName#RECORD} carrying the two nested string sub-fields.</p>
     */
    @Test
    public void shouldTestShouldConvertNestedField() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(TestProtoUtil.createProtoField("field1_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field2_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    nestedBQFields));
        }});


        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(nestedBQFields.size(), fields.get(1).getSubFields().size());

        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(nestedBQFields.get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(0));
        assertBqField(nestedBQFields.get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(1));

    }


    /**
     * Verifies recursive conversion of multiple levels of message nesting.
     *
     * <p>Given a message containing further message fields that each embed the same two nested string
     * fields, when the schema is generated, then the nested records preserve their sub-field counts at
     * every level and the deepest fields convert as expected.</p>
     */
    @Test
    public void shouldTestShouldConvertMultiNestedFields() {
        List<ProtoField> nestedBQFields = new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }};

        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(TestProtoUtil.createProtoField(
                    "field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {{
                        add(TestProtoUtil.createProtoField(
                                "field1_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(TestProtoUtil.createProtoField(
                                "field2_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                        add(TestProtoUtil.createProtoField(
                                "field3_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(TestProtoUtil.createProtoField(
                                "field4_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                    }}
            ));
        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);


        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(4, fields.get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(3).getSubFields().size());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(1).getSubFields());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(3).getSubFields());
    }

    /**
     * Verifies that the well-known timestamp message maps to a BigQuery {@code TIMESTAMP}.
     *
     * <p>Given a single timestamp-typed message field, when the schema is generated, then the field is
     * a {@code NULLABLE} {@link LegacySQLTypeName#TIMESTAMP}.</p>
     */
    @Test
    public void shouldTestConvertToSchemaForTimestamp() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_timestamp",
                    Constants.ProtobufTypeName.TIMESTAMP_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.TIMESTAMP, Field.Mode.NULLABLE, fields.get(0));
    }


    /**
     * Verifies conversion of the special struct, bytes, duration and date types.
     *
     * <p>Given a struct field, a bytes field and the well-known duration and date messages, when the
     * schema is generated, then the struct maps to {@link LegacySQLTypeName#STRING}, the bytes to
     * {@link LegacySQLTypeName#BYTES}, and the duration and date each to a
     * {@link LegacySQLTypeName#RECORD} with their respective integer sub-fields.</p>
     */
    @Test
    public void shouldTestConvertToSchemaForSpecialFields() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_struct",
                    Constants.ProtobufTypeName.STRUCT_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_bytes",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(TestProtoUtil.createProtoField("field3_duration",
                    "." + com.google.protobuf.Duration.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(TestProtoUtil.createProtoField("duration_seconds",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("duration_nanos",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

            add(TestProtoUtil.createProtoField("field3_date",
                    "." + com.google.type.Date.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(TestProtoUtil.createProtoField("year",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("month",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("day",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.BYTES, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(protoField.getFields().get(2).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(2));
        assertBqField(protoField.getFields().get(3).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(3));
        assertEquals(2, fields.get(2).getSubFields().size());
        assertBqField("duration_seconds", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(0));
        assertBqField("duration_nanos", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(1));

        assertEquals(3, fields.get(3).getSubFields().size());
        assertBqField("year", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(0));
        assertBqField("month", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(1));
        assertBqField("day", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(2));
    }

    /**
     * Verifies that repeated scalar fields map to repeated BigQuery columns.
     *
     * <p>Given a repeated int32 field and a repeated string field, when the schema is generated, then
     * each maps to a {@link Field.Mode#REPEATED} column of the corresponding type.</p>
     */
    @Test
    public void shouldTestConvertToSchemaForRepeatedFields() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_map",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));
            add(TestProtoUtil.createProtoField("field2_repeated",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));

        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.INTEGER, Field.Mode.REPEATED, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.REPEATED, fields.get(1));
    }

    /**
     * Asserts that each BigQuery field matches the name, type and {@code NULLABLE} mode of the
     * corresponding proto field.
     *
     * @param pfields  the expected proto fields, in order
     * @param bqFields the produced BigQuery fields to verify against {@code pfields}
     */
    public void assertMultipleFields(List<ProtoField> pfields, List<Field> bqFields) {
        IntStream.range(0, bqFields.size())
                .forEach(index -> {
                    assertBqField(pfields.get(index).getName(), expectedType.get(pfields.get(index).getType()), Field.Mode.NULLABLE, bqFields.get(index));
                });
    }

    /**
     * Asserts that a BigQuery field has the expected name, type and mode.
     *
     * @param name  the expected field name
     * @param ftype the expected BigQuery type
     * @param mode  the expected field mode
     * @param bqf   the BigQuery field under assertion
     */
    public void assertBqField(String name, LegacySQLTypeName ftype, Field.Mode mode, Field bqf) {
        assertEquals(mode, bqf.getMode());
        assertEquals(name, bqf.getName());
        assertEquals(ftype, bqf.getType());
    }


}
