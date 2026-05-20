package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestNestedMessage;
import com.gotocompany.depot.TestTypesMessage;
import dev.cel.runtime.CelRuntime;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProtoMappingParserTest {

    @Test
    public void shouldCompileDirectFieldAccessExpression() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"source.order_number\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        Map<String, CelRuntime.Program> programs = parser.getCompiledPrograms();
        assertEquals(1, programs.size());
        assertNotNull(programs.get("order_number"));
    }

    @Test
    public void shouldCompileMultipleFieldMappings() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{"
                + "\"order_number\": \"source.order_number\","
                + "\"order_url\": \"source.order_url\","
                + "\"order_details\": \"source.order_details\""
                + "}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        Map<String, CelRuntime.Program> programs = parser.getCompiledPrograms();
        assertEquals(3, programs.size());
        assertNotNull(programs.get("order_number"));
        assertNotNull(programs.get("order_url"));
        assertNotNull(programs.get("order_details"));
    }

    @Test
    public void shouldCompileStringConcatenationExpression() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"source.order_number + '-suffix'\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
        assertNotNull(parser.getCompiledPrograms().get("order_number"));
    }

    @Test
    public void shouldCompileStaticConstantExpression() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"'static-value'\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileTernaryExpression() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"source.order_number == '' "
                + "? 'default' : source.order_number\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileHasPresenceCheckExpression() {
        Descriptors.Descriptor descriptor = TestNestedMessage.getDescriptor();
        String mapping = "{\"nested_id\": \"has(source.single_message) "
                + "? source.nested_id : 'missing'\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileNestedFieldAccessExpression() {
        Descriptors.Descriptor descriptor = TestNestedMessage.getDescriptor();
        String mapping = "{\"nested_id\": \"source.single_message.order_number\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileSizeExpression() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"string(source.order_number.size())\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldSetSourceBindingNameToSourceConstant() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"source.order_number\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals("source", parser.getSourceBindingName());
    }

    @Test
    public void shouldSetSourceBindingNameForNestedMessageDescriptor() {
        Descriptors.Descriptor descriptor = TestNestedMessage.getDescriptor();
        String mapping = "{\"nested_id\": \"source.nested_id\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals("source", parser.getSourceBindingName());
    }

    @Test
    public void shouldHandleEmptyMappingJson() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertTrue(parser.getCompiledPrograms().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidCelSyntax() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"invalid @@@ syntax\"}";

        new ProtoMappingParser(mapping, descriptor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnUnknownFieldReference() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{\"order_number\": \"source.nonexistent_field\"}";

        new ProtoMappingParser(mapping, descriptor);
    }

    @Test(expected = org.json.JSONException.class)
    public void shouldThrowOnMalformedJson() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "not-json-at-all";

        new ProtoMappingParser(mapping, descriptor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnTypeMismatchExpression() {
        Descriptors.Descriptor descriptor = TestTypesMessage.getDescriptor();
        String mapping = "{\"string_value\": \"source.int32_value + source.string_value\"}";

        new ProtoMappingParser(mapping, descriptor);
    }

    @Test
    public void shouldCompileBooleanExpression() {
        Descriptors.Descriptor descriptor = TestTypesMessage.getDescriptor();
        String mapping = "{\"bool_value\": \"source.int32_value > 10\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileArithmeticExpression() {
        Descriptors.Descriptor descriptor = TestTypesMessage.getDescriptor();
        String mapping = "{\"int32_value\": \"source.int32_value + 100\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldCompileWithIntToStringConversion() {
        Descriptors.Descriptor descriptor = TestTypesMessage.getDescriptor();
        String mapping = "{\"string_value\": \"string(source.uint32_value)\"}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertEquals(1, parser.getCompiledPrograms().size());
    }

    @Test
    public void shouldHandleEmptyMappingJsonGracefully() {
        Descriptors.Descriptor descriptor = TestMessage.getDescriptor();
        String mapping = "{}";

        ProtoMappingParser parser = new ProtoMappingParser(mapping, descriptor);

        assertTrue(parser.getCompiledPrograms().isEmpty());
        assertEquals("source", parser.getSourceBindingName());
    }

}
