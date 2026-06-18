package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMapMessage;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ProtoSchema}, the Protobuf-backed {@link Schema} adapter.
 *
 * <p>The cases assert that {@link ProtoSchema} surfaces a message descriptor's full name and fields,
 * resolves fields by name, and derives the correct {@link LogicalType} for well-known Protobuf types
 * — timestamps, durations, structs and map entries — as well as for ordinary messages. The shared
 * {@link #schema} wraps {@link TestTypesMessage}, while the map case builds a separate schema over
 * {@link TestMapMessage}.
 */
public class ProtoSchemaTest {
    /**
     * Descriptor of {@link TestTypesMessage} backing the shared schema under test.
     */
    private final Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.getDescriptor();
    /**
     * The {@link ProtoSchema} under test, wrapping {@link TestTypesMessage}.
     */
    private Schema schema = new ProtoSchema(testMessageDescriptor);

    /**
     * Verifies that the schema reports the message type's fully qualified name.
     *
     * <p>Asserts {@link ProtoSchema#getFullName()} returns
     * {@code "com.gotocompany.depot.TestTypesMessage"}.
     */
    @Test
    public void shouldReturnFullName() {
        assertEquals("com.gotocompany.depot.TestTypesMessage", schema.getFullName());
    }

    /**
     * Verifies that all declared fields are exposed.
     *
     * <p>Asserts {@link ProtoSchema#getFields()} returns the 24 fields declared by
     * {@link TestTypesMessage}.
     */
    @Test
    public void shouldReturnAllFieldsPresentInProtoSchema() {
        List<SchemaField> fields = schema.getFields();
        assertEquals(24, fields.size());
    }

    /**
     * Verifies that a field can be resolved by name with its mapped type.
     *
     * <p>Looks up {@code sint32_value} via {@link ProtoSchema#getFieldByName(String)} and asserts its
     * type is {@link SchemaFieldType#INT}.
     */
    @Test
    public void shouldReturnSchemaFieldGivenAFieldName() {
        SchemaField sint32Field = schema.getFieldByName("sint32_value");
        assertEquals(SchemaFieldType.INT, sint32Field.getType());
    }

    /**
     * Verifies that a timestamp field is recognized as the timestamp logical type.
     *
     * <p>Resolves {@code timestamp_value}, asserts its type is {@link SchemaFieldType#MESSAGE}, and
     * asserts its nested value type reports {@link LogicalType#TIMESTAMP}.
     */
    @Test
    public void shouldReturnTimestampLogicalTypeForTimestampFields() {
        SchemaField timestampField = schema.getFieldByName("timestamp_value");
        assertEquals(SchemaFieldType.MESSAGE, timestampField.getType());
        assertEquals(LogicalType.TIMESTAMP, timestampField.getValueType().logicalType());
    }

    /**
     * Verifies that a duration field is recognized as the duration logical type.
     *
     * <p>Resolves {@code duration_value}, asserts its type is {@link SchemaFieldType#MESSAGE}, and
     * asserts its nested value type reports {@link LogicalType#DURATION}.
     */
    @Test
    public void shouldReturnDurationLogicalTypeForDurationField() {
        SchemaField durationField = schema.getFieldByName("duration_value");
        assertEquals(SchemaFieldType.MESSAGE, durationField.getType());
        assertEquals(LogicalType.DURATION, durationField.getValueType().logicalType());
    }

    /**
     * Verifies that a struct field is recognized as the struct logical type.
     *
     * <p>Resolves {@code struct_value}, asserts its type is {@link SchemaFieldType#MESSAGE}, and
     * asserts its nested value type reports {@link LogicalType#STRUCT}.
     */
    @Test
    public void shouldReturnStructLogicalTypeForStructFields() {
        SchemaField structField = schema.getFieldByName("struct_value");
        assertEquals(SchemaFieldType.MESSAGE, structField.getType());
        assertEquals(LogicalType.STRUCT, structField.getValueType().logicalType());
    }

    /**
     * Verifies that a map field's entry type is recognized as the map logical type.
     *
     * <p>Builds a {@link ProtoSchema} over {@link TestMapMessage}, resolves {@code current_state}, and
     * asserts its type is {@link SchemaFieldType#MESSAGE} with a nested value type reporting
     * {@link LogicalType#MAP}, while the enclosing schema itself reports {@link LogicalType#MESSAGE}.
     */
    @Test
    public void shouldReturnMapLogicalTypeForMapFieldGeneratedMessage() {
        Schema mapSchema = new ProtoSchema(TestMapMessage.getDescriptor());
        SchemaField currentState = mapSchema.getFieldByName("current_state");
        assertEquals(SchemaFieldType.MESSAGE, currentState.getType());
        assertEquals(LogicalType.MAP, currentState.getValueType().logicalType());
        assertEquals(LogicalType.MESSAGE, mapSchema.logicalType());
    }
}
