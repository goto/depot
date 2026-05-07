package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestNestedMessage;
import com.gotocompany.depot.TestTypesMessage;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ProtoMappingFunctionTest {

    @Test
    public void shouldMapDirectFieldFromSourceToSink() throws CelEvaluationException {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("ORD-123").setOrderUrl("http://test.com").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertNotNull(result.getMessage());
        assertEquals("ORD-123", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
        assertNull(result.getKey());
    }

    @Test
    public void shouldMapMultipleFieldsFromSourceToSink() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{"
                + "\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\","
                + "\"order_url\": \"com.gotocompany.depot.TestMessage.order_url\","
                + "\"order_details\": \"com.gotocompany.depot.TestMessage.order_details\""
                + "}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder()
                .setOrderNumber("ORD-456")
                .setOrderUrl("http://example.com")
                .setOrderDetails("details here")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("ORD-456", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
        assertEquals("http://example.com", result.getMessage().getField(sinkDesc.findFieldByName("order_url")));
        assertEquals("details here", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapFieldToKeyProto() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkMessageDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkKeyDesc = TestKey.getDescriptor();

        String mapping = "{"
                + "\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\""
                + "}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkMessageDesc, sinkKeyDesc);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("KEY-789").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertNotNull(result.getKey());
        assertEquals("KEY-789", result.getKey().getField(sinkKeyDesc.findFieldByName("order_number")));
        assertEquals("KEY-789", result.getMessage().getField(sinkMessageDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapStaticConstantToField() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"'constant-value'\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("ORD-1").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("constant-value", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapStringConcatenation() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"com.gotocompany.depot.TestMessage.order_number + '-' + com.gotocompany.depot.TestMessage.order_url\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder()
                .setOrderNumber("ORD-1")
                .setOrderUrl("http://test.com")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("ORD-1-http://test.com", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapTernaryExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"com.gotocompany.depot.TestMessage.order_number == '' "
                + "? 'empty' : com.gotocompany.depot.TestMessage.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("empty", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapTernaryExpressionNonEmptyBranch() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"com.gotocompany.depot.TestMessage.order_number == '' "
                + "? 'empty' : com.gotocompany.depot.TestMessage.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("has-value").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("has-value", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldHandleEmptySourceMessageWithDefaults() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapNestedFieldAccess() throws Exception {
        Descriptors.Descriptor sourceDesc = TestNestedMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestNestedMessage.single_message.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestNestedMessage source = TestNestedMessage.newBuilder()
                .setNestedId("N1")
                .setSingleMessage(TestMessage.newBuilder().setOrderNumber("NESTED-ORD").build())
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("NESTED-ORD", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapWithEmptyMappingProducingDefaultOutput() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("ORD-1").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertNotNull(result.getMessage());
        assertEquals("", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldProduceNullKeyWhenNoKeyDescriptorProvided() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("ORD-X").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertNull(result.getKey());
    }

    @Test
    public void shouldMapFieldOnlyToKeyWhenFieldExistsOnlyInKeyDescriptor() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkMessageDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkKeyDesc = TestKey.getDescriptor();

        String mapping = "{\"order_url\": \"com.gotocompany.depot.TestMessage.order_url\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkMessageDesc, sinkKeyDesc);

        TestMessage source = TestMessage.newBuilder().setOrderUrl("http://key-url.com").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("http://key-url.com", result.getMessage().getField(sinkMessageDesc.findFieldByName("order_url")));
        assertEquals("http://key-url.com", result.getKey().getField(sinkKeyDesc.findFieldByName("order_url")));
    }

    @Test
    public void shouldMapWithStartsWithExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"com.gotocompany.depot.TestMessage.order_number.startsWith('PRE') "
                + "? 'prefixed' : 'unprefixed'\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("PRE-TEST").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("prefixed", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapWithEndsWithExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"com.gotocompany.depot.TestMessage.order_number.endsWith('-TEST') "
                + "? 'has-suffix' : 'no-suffix'\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("ORDER-TEST").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("has-suffix", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapWithNestedMessageField() throws Exception {
        Descriptors.Descriptor sourceDesc = TestNestedMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_url\": \"com.gotocompany.depot.TestNestedMessage.single_message.order_url\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage nested = TestMessage.newBuilder()
                .setOrderNumber("NEST-001")
                .setOrderUrl("http://nested.example.com")
                .setOrderDetails("nested details")
                .build();
        TestNestedMessage source = TestNestedMessage.newBuilder()
                .setNestedId("parent-id")
                .setSingleMessage(nested)
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("http://nested.example.com", result.getMessage().getField(sinkDesc.findFieldByName("order_url")));
    }

    @Test
    public void shouldMapWithMultipleExpressionsToDifferentFields() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{"
                + "\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\","
                + "\"order_url\": \"'http://static.url'\","
                + "\"order_details\": \"com.gotocompany.depot.TestMessage.order_number + '-details'\""
                + "}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder()
                .setOrderNumber("MULTI-1")
                .setOrderUrl("http://source.url")
                .setOrderDetails("original-details")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("MULTI-1", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
        assertEquals("http://static.url", result.getMessage().getField(sinkDesc.findFieldByName("order_url")));
        assertEquals("MULTI-1-details", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapWithBooleanComparisonExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestTypesMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestTypesMessage.int32_value > 50 "
                + "? 'high' : 'low'\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestTypesMessage source = TestTypesMessage.newBuilder().setInt32Value(75).build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("high", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapWithBooleanComparisonExpressionFalse() throws Exception {
        Descriptors.Descriptor sourceDesc = TestTypesMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestTypesMessage.int32_value > 50 "
                + "? 'high' : 'low'\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestTypesMessage source = TestTypesMessage.newBuilder().setInt32Value(25).build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("low", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapWithArithmeticExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestTypesMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"string(com.gotocompany.depot.TestTypesMessage.int32_value * 2 + 100)\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestTypesMessage source = TestTypesMessage.newBuilder().setInt32Value(50).build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("200", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapKeyOnlyWhenFieldExistsInKeyButNotMessage() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkMessageDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkKeyDesc = TestKey.getDescriptor();

        String mapping = "{\"order_url\": \"com.gotocompany.depot.TestMessage.order_url\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkMessageDesc, sinkKeyDesc);

        TestMessage source = TestMessage.newBuilder().setOrderUrl("http://key-only-url.com").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertNull(result.getMessage().getField(sinkMessageDesc.findFieldByName("order_url")));
        assertEquals("http://key-only-url.com", result.getKey().getField(sinkKeyDesc.findFieldByName("order_url")));
    }

    @Test
    public void shouldMapBothMessageAndKeyFieldsIndependently() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkMessageDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkKeyDesc = TestKey.getDescriptor();

        String mapping = "{"
                + "\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\","
                + "\"order_url\": \"com.gotocompany.depot.TestMessage.order_url\""
                + "}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkMessageDesc, sinkKeyDesc);

        TestMessage source = TestMessage.newBuilder()
                .setOrderNumber("INDEP-KEY")
                .setOrderUrl("http://indep-url.com")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("INDEP-KEY", result.getMessage().getField(sinkMessageDesc.findFieldByName("order_number")));
        assertEquals("INDEP-KEY", result.getKey().getField(sinkKeyDesc.findFieldByName("order_number")));
        assertEquals("http://indep-url.com", result.getMessage().getField(sinkMessageDesc.findFieldByName("order_url")));
        assertEquals("http://indep-url.com", result.getKey().getField(sinkKeyDesc.findFieldByName("order_url")));
    }

    @Test(expected = CelEvaluationException.class)
    public void shouldThrowCelEvaluationExceptionForRuntimeError() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number.toUpperCase()\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        function.map(sourceMsg);
    }

    @Test
    public void shouldMapWithStringSizeExpression() throws Exception {
        Descriptors.Descriptor sourceDesc = TestMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{\"order_details\": \"string(com.gotocompany.depot.TestMessage.order_number.size())\"}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestMessage source = TestMessage.newBuilder().setOrderNumber("EXACTLY-8-CHARS").build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("15", result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

    @Test
    public void shouldMapWithMultipleFieldsAndDifferentDataTypes() throws Exception {
        Descriptors.Descriptor sourceDesc = TestTypesMessage.getDescriptor();
        Descriptors.Descriptor sinkDesc = TestMessage.getDescriptor();

        String mapping = "{"
                + "\"order_number\": \"string(com.gotocompany.depot.TestTypesMessage.int32_value)\","
                + "\"order_url\": \"string(com.gotocompany.depot.TestTypesMessage.int64_value)\","
                + "\"order_details\": \"string(com.gotocompany.depot.TestTypesMessage.float_value)\""
                + "}";
        ProtoMappingParser parser = new ProtoMappingParser(mapping, sourceDesc);

        ProtoMappingFunction function = new ProtoMappingFunction(
                parser.getCompiledPrograms(), parser.getSourceBindingName(), sinkDesc, null);

        TestTypesMessage source = TestTypesMessage.newBuilder()
                .setInt32Value(123)
                .setInt64Value(456789L)
                .setFloatValue(3.14f)
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        ProtoMappingFunction.MappedMessages result = function.map(sourceMsg);

        assertEquals("123", result.getMessage().getField(sinkDesc.findFieldByName("order_number")));
        assertEquals("456789", result.getMessage().getField(sinkDesc.findFieldByName("order_url")));
        assertNotNull(result.getMessage().getField(sinkDesc.findFieldByName("order_details")));
    }

}
