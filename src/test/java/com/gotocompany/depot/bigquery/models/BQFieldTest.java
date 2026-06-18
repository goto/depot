package com.gotocompany.depot.bigquery.models;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.message.proto.ProtoField;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link BQField}, which maps Protobuf field descriptors to BigQuery {@link Field}
 * definitions.
 *
 * <p>Each test resolves a field descriptor from the generated {@link TestTypesMessage} (or
 * {@link TestMessage}) schema, converts it through {@link BQField} and asserts that the resulting
 * BigQuery type, mode and nested structure match expectations. Conversions are exercised for every
 * supported Protobuf scalar, the well-known timestamp/struct/duration message types, nested message
 * records and repeated modifiers.</p>
 */
public class BQFieldTest {

    /** Descriptor of {@link TestTypesMessage}, the schema whose fields drive most of the conversions. */
    private final Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.newBuilder().build().getDescriptorForType();

    /**
     * Verifies that a scalar Protobuf field is mapped to the expected nullable BigQuery field.
     *
     * <p>Given the {@code double_value} descriptor, when it is converted to a BigQuery {@link Field},
     * then the result equals a {@code NULLABLE} {@link LegacySQLTypeName#FLOAT} field of the same
     * name.</p>
     */
    @Test
    public void shouldReturnBigqueryField() {
        String fieldName = "double_value";
        Field expected = Field.newBuilder(fieldName, LegacySQLTypeName.FLOAT).setMode(Field.Mode.NULLABLE).build();
        Field field = fieldDescriptorToField(testMessageDescriptor.findFieldByName(fieldName));

        assertEquals(expected, field);
    }

    /**
     * Verifies that a message field with explicitly attached sub-fields becomes a nested record.
     *
     * <p>Given the {@code message_value} descriptor and three child string fields
     * ({@code order_number}, {@code order_url} and {@code order_details}) attached via
     * {@code setSubFields}, when the BigQuery {@link Field} is produced, then it equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#RECORD} whose {@link FieldList} contains the three
     * nullable string sub-fields.</p>
     */
    @Test
    public void shouldReturnBigqueryFieldWithChildField() {
        String fieldName = "message_value";

        TestMessage testMessage = TestMessage.newBuilder().build();
        Descriptors.FieldDescriptor orderNumber = testMessage.getDescriptorForType().findFieldByName("order_number");
        Descriptors.FieldDescriptor orderUrl = testMessage.getDescriptorForType().findFieldByName("order_url");
        Descriptors.FieldDescriptor orderDetails = testMessage.getDescriptorForType().findFieldByName("order_details");

        List<Field> childFields = new ArrayList<>();
        childFields.add(fieldDescriptorToField(orderNumber));
        childFields.add(fieldDescriptorToField(orderUrl));
        childFields.add(fieldDescriptorToField(orderDetails));

        Descriptors.FieldDescriptor messageFieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField bqField = fieldDescriptorToBQField(messageFieldDescriptor);
        bqField.setSubFields(childFields);
        Field field = bqField.getField();

        Field expectedOrderNumberBqField = Field.newBuilder("order_number", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field expectedOrderNumberBqFieldUrl = Field.newBuilder("order_url", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field expectedOrderDetailsBqField1 = Field.newBuilder("order_details", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();

        Field expected = Field.newBuilder(fieldName, LegacySQLTypeName.RECORD,
                FieldList.of(expectedOrderNumberBqField,
                        expectedOrderNumberBqFieldUrl,
                        expectedOrderDetailsBqField1)).setMode(Field.Mode.NULLABLE).build();

        assertEquals(expected, field);
    }


    /**
     * Verifies that a Protobuf well-known timestamp field maps to a BigQuery {@code TIMESTAMP}.
     *
     * <p>Given the {@code timestamp_value} descriptor, when converted to a {@link BQField}, then the
     * resolved BigQuery type is {@link LegacySQLTypeName#TIMESTAMP}.</p>
     */
    @Test
    public void shouldConvertProtobufTimestampToBigqueryTimestamp() {
        String fieldName = "timestamp_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.TIMESTAMP, bqFieldType);
    }

    /**
     * Verifies that a Protobuf {@code Struct} field maps to a BigQuery {@code STRING}.
     *
     * <p>Given the {@code struct_value} descriptor, when converted to a {@link BQField}, then the
     * resolved BigQuery type is {@link LegacySQLTypeName#STRING}, since structs are serialized as JSON
     * strings.</p>
     */
    @Test
    public void shouldConvertProtobufStructToBigqueryString() {
        String fieldName = "struct_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.STRING, bqFieldType);
    }

    /**
     * Verifies that a Protobuf {@code Duration} field maps to a BigQuery {@code RECORD}.
     *
     * <p>Given the {@code duration_value} descriptor, when converted to a {@link BQField}, then the
     * resolved BigQuery type is {@link LegacySQLTypeName#RECORD}.</p>
     */
    @Test
    public void shouldConvertProtobufDurationToBigqueryRecord() {
        String fieldName = "duration_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        LegacySQLTypeName bqFieldType = bqField.getType();

        assertEquals(LegacySQLTypeName.RECORD, bqFieldType);
    }


    /**
     * Verifies that a Protobuf {@code double} field maps to a nullable BigQuery {@code FLOAT} field.
     *
     * <p>Given the {@code double_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#FLOAT} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufDoubleToBigqueryFloat() {
        String fieldName = "double_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.FLOAT, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code float} field maps to a nullable BigQuery {@code FLOAT} field.
     *
     * <p>Given the {@code float_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#FLOAT} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufFloatToBigqueryFloat() {
        String fieldName = "float_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.FLOAT, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code bytes} field maps to a nullable BigQuery {@code BYTES} field.
     *
     * <p>Given the {@code bytes_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#BYTES} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufBytesToBigqueryBytes() {
        String fieldName = "bytes_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.BYTES, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code bool} field maps to a nullable BigQuery {@code BOOLEAN} field.
     *
     * <p>Given the {@code bool_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#BOOLEAN} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufBoolToBigqueryBool() {
        String fieldName = "bool_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.BOOLEAN, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code enum} field maps to a nullable BigQuery {@code STRING} field.
     *
     * <p>Given the {@code enum_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#STRING} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufEnumToBigqueryString() {
        String fieldName = "enum_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code string} field maps to a nullable BigQuery {@code STRING} field.
     *
     * <p>Given the {@code string_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#STRING} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufStringToBigqueryString() {
        String fieldName = "string_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code int64} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code int64_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufInt64ToBigqueryInteger() {
        String fieldName = "int64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code uint64} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code uint64_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufUint64ToBigqueryInteger() {
        String fieldName = "uint64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code int32} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code int32_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufInt32ToBigqueryInteger() {
        String fieldName = "int32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);

        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code uint32} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code uint32_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufUint32ToBigqueryInteger() {
        String fieldName = "uint32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code fixed32} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code fixed32_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufFixed32ToBigqueryInteger() {
        String fieldName = "fixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code fixed64} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code fixed64_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufFixed64ToBigqueryInteger() {
        String fieldName = "fixed64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code sfixed32} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code sfixed32_value} descriptor, when converted, then the {@link BQField} equals
     * a {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufSfixed32ToBigqueryInteger() {
        String fieldName = "sfixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that an {@code sfixed} integer field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>The method name targets the {@code sfixed64} mapping, but the descriptor it actually resolves
     * is {@code sfixed32_value}; either way the conversion yields a {@code NULLABLE}
     * {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufSfixed64ToBigqueryInteger() {
        String fieldName = "sfixed32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code sint32} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code sint32_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufSint32ToBigqueryInteger() {
        String fieldName = "sint32_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf {@code sint64} field maps to a nullable BigQuery {@code INTEGER} field.
     *
     * <p>Given the {@code sint64_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#INTEGER} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufSint64ToBigqueryInteger() {
        String fieldName = "sint64_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.INTEGER, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a Protobuf message field maps to a nullable BigQuery {@code RECORD} field.
     *
     * <p>Given the {@code message_value} descriptor, when converted, then the {@link BQField} equals a
     * {@code NULLABLE} {@link LegacySQLTypeName#RECORD} field of the same name (with sub-fields
     * resolved separately).</p>
     */
    @Test
    public void shouldConvertProtobufMessageTypeToBigqueryRecord() {
        String fieldName = "message_value";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.NULLABLE, LegacySQLTypeName.RECORD, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a {@code repeated} scalar field maps to a repeated BigQuery column.
     *
     * <p>Given the {@code list_values} descriptor, when converted, then the {@link BQField} equals a
     * {@link Field.Mode#REPEATED} {@link LegacySQLTypeName#STRING} field of the same name.</p>
     */
    @Test
    public void shouldConvertProtobufRepeatedModifierToBigqueryRepeatedColumn() {
        String fieldName = "list_values";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.REPEATED, LegacySQLTypeName.STRING, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Verifies that a {@code repeated} message field maps to a repeated BigQuery record column.
     *
     * <p>Given the {@code list_message_values} descriptor, when converted, then the {@link BQField}
     * equals a {@link Field.Mode#REPEATED} {@link LegacySQLTypeName#RECORD} field of the same
     * name.</p>
     */
    @Test
    public void shouldConvertProtobufRepeatedMessageModifierToBigqueryRepeatedRecordColumn() {
        String fieldName = "list_message_values";
        Descriptors.FieldDescriptor fieldDescriptor = testMessageDescriptor.findFieldByName(fieldName);
        BQField expected = new BQField(fieldName, Field.Mode.REPEATED, LegacySQLTypeName.RECORD, new ArrayList<>());
        BQField result = fieldDescriptorToBQField(fieldDescriptor);

        assertEquals(expected, result);
    }

    /**
     * Converts a Protobuf field descriptor into its BigQuery {@link Field} representation.
     *
     * @param fieldDescriptor the Protobuf field descriptor to convert
     * @return the BigQuery {@link Field} produced by wrapping the descriptor in a {@link BQField}
     */
    private Field fieldDescriptorToField(Descriptors.FieldDescriptor fieldDescriptor) {
        BQField bqField = fieldDescriptorToBQField(fieldDescriptor);
        return bqField.getField();
    }

    /**
     * Wraps a Protobuf field descriptor in a {@link BQField} via an intermediate {@link ProtoField}.
     *
     * @param fieldDescriptor the Protobuf field descriptor to wrap
     * @return a {@link BQField} backed by the supplied descriptor
     */
    private BQField fieldDescriptorToBQField(Descriptors.FieldDescriptor fieldDescriptor) {
        ProtoField protoField = new ProtoField(fieldDescriptor.toProto());
        return new BQField(protoField);
    }

}
