package com.gotocompany.depot.schema.proto;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.schema.SchemaFieldType;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Unit tests for {@link ProtoSchemaField}, the Protobuf-backed
 * {@link com.gotocompany.depot.schema.SchemaField} adapter.
 *
 * <p>The class runs with the JUnit {@link Enclosed} runner and groups its cases into two nested
 * suites: {@link SingleRunTests} for one-off assertions about message value types, and the
 * parameterized {@link SchemaFieldTypeTest} that maps every Protobuf field type of
 * {@link TestTypesMessage} to its expected {@link SchemaFieldType}. The shared {@link #TEST_SCHEMA}
 * descriptor provides the fields under test.
 */
@RunWith(Enclosed.class)
public class ProtoSchemaFieldTest {
    /**
     * Descriptor of {@link TestTypesMessage}, shared as the source of the fields exercised by the
     * tests.
     */
    private static final Descriptors.Descriptor TEST_SCHEMA = TestTypesMessage.getDescriptor();

    /**
     * Non-parameterized cases covering {@link ProtoSchemaField#getValueType()} for message and
     * non-message fields.
     */
    public static class SingleRunTests {

        /**
         * Verifies that a non-message field has no nested value type.
         *
         * <p>Wraps the {@code string_value} field in a {@link ProtoSchemaField} and asserts
         * {@link ProtoSchemaField#getValueType()} returns {@code null}.
         */
        @Test
        public void getValueTypeShouldReturnNullIfFieldIsNotMessageType() {
            Descriptors.FieldDescriptor stringField = TEST_SCHEMA.findFieldByName("string_value");
            ProtoSchemaField schemaField = new ProtoSchemaField(stringField);
            assertNull(schemaField.getValueType());
        }

        /**
         * Verifies that a message field exposes the nested message's schema.
         *
         * <p>Wraps the {@code message_value} field in a {@link ProtoSchemaField} and asserts
         * {@link ProtoSchemaField#getValueType()} is non-null and reports the full name
         * {@code "com.gotocompany.depot.TestMessage"}.
         */
        @Test
        public void getValueTypeShouldReturnSchemaOfMessageType() {
            Descriptors.FieldDescriptor stringField = TEST_SCHEMA.findFieldByName("message_value");
            ProtoSchemaField schemaField = new ProtoSchemaField(stringField);
            assertNotNull(schemaField.getValueType());
            assertEquals("com.gotocompany.depot.TestMessage", schemaField.getValueType().getFullName());
        }

    }

    /**
     * Parameterized cases asserting how each Protobuf field type maps onto a {@link SchemaFieldType}.
     *
     * <p>Driven by {@link #testCases()}, each row supplies a field name on {@link TestTypesMessage},
     * its expected {@link SchemaFieldType}, and whether the field is repeated; the tests assert the
     * type, name, JSON name and repeated flag exposed by the corresponding {@link ProtoSchemaField}.
     */
    @RunWith(Parameterized.class)
    public static class SchemaFieldTypeTest {
        /**
         * Descriptor of {@link TestTypesMessage} from which the parameterized field is resolved.
         */
        private final Descriptors.Descriptor testMessageDescriptor = TestTypesMessage.getDescriptor();
        /**
         * Name of the Protobuf field under test for this row.
         */
        private String fieldName;
        /**
         * The {@link SchemaFieldType} this field is expected to map to.
         */
        private SchemaFieldType expectedType;
        /**
         * Whether the field under test is expected to be repeated.
         */
        private boolean repeated;

        /**
         * Creates a parameterized case for a single field-type mapping row.
         *
         * @param fieldName    the Protobuf field name to resolve and wrap
         * @param expectedType the {@link SchemaFieldType} the field should map to
         * @param repeated     whether the field is expected to be repeated
         */
        public SchemaFieldTypeTest(String fieldName, SchemaFieldType expectedType, boolean repeated) {
            this.fieldName = fieldName;
            this.expectedType = expectedType;
            this.repeated = repeated;
        }

        /**
         * Supplies the field-name, expected-type and repeated-flag rows for this suite.
         *
         * <p>Covers every scalar Protobuf type, enums, singular and repeated messages, and a repeated
         * scalar, mapping each to the {@link SchemaFieldType} that {@link ProtoSchemaField} is expected
         * to report.
         *
         * @return the parameter rows driving {@link #testSchemaTypeMapping()} and its sibling tests
         */
        @Parameterized.Parameters(name = "{index}: {0} should return type {1}. isRepeated: {2}")
        public static Iterable<Object[]> testCases() {
            return Arrays.asList(new Object[][]{
                    {"double_value", SchemaFieldType.DOUBLE, false},
                    {"bytes_value", SchemaFieldType.BYTES, false},
                    {"float_value", SchemaFieldType.FLOAT, false},
                    {"int32_value", SchemaFieldType.INT, false},
                    {"int64_value", SchemaFieldType.LONG, false},
                    {"uint32_value", SchemaFieldType.INT, false},
                    {"uint64_value", SchemaFieldType.LONG, false},
                    {"fixed32_value", SchemaFieldType.INT, false},
                    {"fixed64_value", SchemaFieldType.LONG, false},
                    {"sfixed32_value", SchemaFieldType.INT, false},
                    {"sfixed64_value", SchemaFieldType.LONG, false},
                    {"sint32_value", SchemaFieldType.INT, false},
                    {"sint64_value", SchemaFieldType.LONG, false},
                    {"enum_value", SchemaFieldType.ENUM, false},
                    {"string_value", SchemaFieldType.STRING, false},
                    {"message_value", SchemaFieldType.MESSAGE, false},
                    {"bool_value", SchemaFieldType.BOOLEAN, false},
                    {"duration_value", SchemaFieldType.MESSAGE, false},
                    {"list_values", SchemaFieldType.STRING, true},
                    {"list_message_values", SchemaFieldType.MESSAGE, true}
            });
        }

        /**
         * Verifies that the field's reported type matches the expected {@link SchemaFieldType}.
         *
         * <p>Resolves {@code fieldName} on {@link TestTypesMessage}, wraps it in a
         * {@link ProtoSchemaField}, and asserts {@link ProtoSchemaField#getType()} equals the expected
         * type for the row.
         */
        @Test
        public void testSchemaTypeMapping() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(expectedType, schemaField.getType());
        }

        /**
         * Verifies that the field reports its declared Protobuf name.
         *
         * <p>Resolves {@code fieldName}, wraps it in a {@link ProtoSchemaField}, and asserts
         * {@link ProtoSchemaField#getName()} equals the original field name.
         */
        @Test
        public void testGetName() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(fieldName, schemaField.getName());
        }

        /**
         * Verifies that the field reports the Protobuf-derived JSON name.
         *
         * <p>Resolves {@code fieldName}, wraps it in a {@link ProtoSchemaField}, and asserts
         * {@link ProtoSchemaField#getJsonName()} matches the descriptor's own JSON name.
         */
        @Test
        public void testGetJsonName() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(doubleField.getJsonName(), schemaField.getJsonName());
        }

        /**
         * Verifies that the field reports its repeated status.
         *
         * <p>Resolves {@code fieldName}, wraps it in a {@link ProtoSchemaField}, and asserts
         * {@link ProtoSchemaField#isRepeated()} equals the expected repeated flag for the row.
         */
        @Test
        public void testIsRepeated() {
            Descriptors.FieldDescriptor doubleField = testMessageDescriptor.findFieldByName(fieldName);
            ProtoSchemaField schemaField = new ProtoSchemaField(doubleField);
            assertEquals(repeated, schemaField.isRepeated());
        }
    }
}
