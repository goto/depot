package com.gotocompany.depot.schema.json;

import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link GenericJsonSchemaField}, the field type inferred from a JSON value.
 *
 * <p>The class runs with the JUnit {@link Enclosed} runner. Its nested, parameterized
 * {@link JSONSchemaFieldTypeTest} suite resolves each field of a shared {@link #TEST_SCHEMA} (built
 * over {@link #TEST_OBJ}) and asserts the {@link SchemaFieldType}, repeated flag and JSON name that
 * {@link GenericJsonSchemaField} infers, including how arrays — populated and empty — and numeric
 * literals are typed.
 */
@RunWith(Enclosed.class)
public class GenericJsonSchemaFieldTest {

    /**
     * JSON document fixture covering scalars, a nested object, a null, numeric literals, and populated
     * and empty arrays.
     */
    private static final JSONObject TEST_OBJ = new JSONObject("{\"intField\":1,\"stringField\":\"value\",\"doubleField\":10.2,\"nested\":{\"a\":1},\"boolField\":true,\"nullField\":null,\"floatField\":2.99792458e8,\"f\":1.1E+2, \"array\": [1,2], \"empty_array\": []}");
    /**
     * Schema inferred from {@link #TEST_OBJ}, queried by the parameterized cases.
     */
    private static final Schema TEST_SCHEMA = new GenericJsonSchema(TEST_OBJ);

    /**
     * Parameterized cases asserting the type, repeated flag and JSON name inferred for each JSON value.
     *
     * <p>Each row from {@link #testCases()} names a field of {@code TEST_OBJ}, its expected
     * {@link SchemaFieldType}, and whether it is repeated; the tests resolve the field from
     * {@code TEST_SCHEMA} and assert the inferred metadata.
     */
    @RunWith(Parameterized.class)
    public static class JSONSchemaFieldTypeTest {
        /**
         * Name of the JSON field under test for this row.
         */
        private final String fieldName;
        /**
         * The {@link SchemaFieldType} the field is expected to map to.
         */
        private final SchemaFieldType expectedType;
        /**
         * Whether the field under test is expected to be repeated.
         */
        private final boolean repeated;

        /**
         * Creates a parameterized case for a single inferred-field row.
         *
         * @param fieldName    the JSON field name to resolve
         * @param expectedType the {@link SchemaFieldType} the field should map to
         * @param repeated     whether the field is expected to be repeated
         */
        public JSONSchemaFieldTypeTest(String fieldName, SchemaFieldType expectedType, boolean repeated) {
            this.fieldName = fieldName;
            this.expectedType = expectedType;
            this.repeated = repeated;
        }

        /**
         * Supplies the field-name, expected-type and repeated-flag rows for this suite.
         *
         * <p>Covers scalar values, a nested object, numeric literals that infer as
         * {@link SchemaFieldType#DOUBLE}, a populated array (typed from its first element) and an empty
         * array (defaulting to {@link SchemaFieldType#STRING}).
         *
         * @return the parameter rows driving the inferred-field assertions
         */
        @Parameterized.Parameters(name = "{index}: {0} should return type {1}. isRepeated: {2}")
        public static Iterable<Object[]> testCases() {
            return Arrays.asList(new Object[][]{
                    {"intField", SchemaFieldType.INT, false},
                    {"stringField", SchemaFieldType.STRING, false},
                    {"doubleField", SchemaFieldType.DOUBLE, false},
                    {"boolField", SchemaFieldType.BOOLEAN, false},
                    {"floatField", SchemaFieldType.DOUBLE, false},
                    {"nested", SchemaFieldType.MESSAGE, false},
                    {"f", SchemaFieldType.DOUBLE, false},
                    {"array", SchemaFieldType.INT, true},
                    {"empty_array", SchemaFieldType.STRING, true}
            });
        }

        /**
         * Verifies that the inferred field type matches the expected {@link SchemaFieldType}.
         *
         * <p>Resolves {@code fieldName} from {@code TEST_SCHEMA} and asserts
         * {@link GenericJsonSchemaField#getType()} equals the expected type for the row.
         */
        @Test
        public void testSchemaTypeMapping() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(expectedType, schemaField.getType());
        }

        /**
         * Verifies that the field reports its repeated status.
         *
         * <p>Resolves {@code fieldName} from {@code TEST_SCHEMA} and asserts
         * {@link GenericJsonSchemaField#isRepeated()} equals the expected repeated flag, distinguishing
         * array values from scalars.
         */
        @Test
        public void testIsRepeated() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(repeated, schemaField.isRepeated());
        }

        /**
         * Verifies that the field's JSON name equals its key.
         *
         * <p>Resolves {@code fieldName} from {@code TEST_SCHEMA} and asserts
         * {@link GenericJsonSchemaField#getJsonName()} returns the same field name.
         */
        @Test
        public void testGetJsonName() {
            SchemaField schemaField = TEST_SCHEMA.getFieldByName(fieldName);
            assertEquals(fieldName, schemaField.getJsonName());
        }
    }
}
