package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link GenericJsonSchema}, the schema that infers its structure from a
 * {@link JSONObject}.
 *
 * <p>The cases assert that {@link GenericJsonSchema} enumerates a document's top-level fields,
 * resolves a field by name with its inferred type, exposes nested objects as further
 * {@link GenericJsonSchema} instances, and always reports {@link LogicalType#MESSAGE}. A shared
 * {@link #testObj} fixture with a mix of value types backs every case.
 */
public class GenericJsonSchemaTest {
    /**
     * JSON document fixture mixing integer, string, double, nested-object, boolean, null and
     * floating-point values.
     */
    private final JSONObject testObj = new JSONObject("{\"intField\":1,\"stringField\":\"value\",\"doubleField\":10.2,\"nested\":{\"a\":1},\"boolField\":true,\"nullField\":null,\"floatField\":1.2E+20,\"f\":1.1E+2}");

    /**
     * Verifies that every top-level field of the document is enumerated.
     *
     * <p>Builds a schema over {@link #testObj}, asserts {@link GenericJsonSchema#getFields()} yields
     * eight fields, and confirms their names match the document's keys exactly, regardless of order.
     */
    @Test
    public void shouldReturnListOfFields() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        List<SchemaField> fields = jsonSchema.getFields();
        assertEquals(8, fields.size());
        List<String> actual = fields.stream().map(SchemaField::getName).collect(Collectors.toList());
        List<String> expected = Arrays.asList("intField", "stringField", "doubleField", "nested", "boolField", "nullField", "floatField", "f");
        assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
    }

    /**
     * Verifies that a field resolved by name carries its inferred type.
     *
     * <p>Looks up {@code intField} via {@link GenericJsonSchema#getFieldByName(String)} and asserts
     * its name is {@code "intField"} and its type is {@link SchemaFieldType#INT}.
     */
    @Test
    public void shouldReturnFieldSchemaForGivenFieldName() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        SchemaField intField = jsonSchema.getFieldByName("intField");
        assertEquals("intField", intField.getName());
        assertEquals(SchemaFieldType.INT, intField.getType());
    }

    /**
     * Verifies that a nested object is exposed as a nested schema.
     *
     * <p>Resolves the {@code nested} field and asserts its value type is a {@link GenericJsonSchema}
     * describing the nested object, which itself reports a single field.
     */
    @Test
    public void shouldReturnParsedMessageTypeForNestedFields() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        SchemaField nestedField = jsonSchema.getFieldByName("nested");
        assertTrue(nestedField.getValueType() instanceof GenericJsonSchema);
        assertEquals(1, nestedField.getValueType().getFields().size());
    }

    /**
     * Verifies that a JSON schema always reports the message logical type.
     *
     * <p>Asserts {@link GenericJsonSchema#logicalType()} returns {@link LogicalType#MESSAGE}.
     */
    @Test
    public void shouldAlwaysReturnMessageLogicalType() {
        Schema jsonSchema = new GenericJsonSchema(testObj);
        assertEquals(LogicalType.MESSAGE, jsonSchema.logicalType());
    }

}
