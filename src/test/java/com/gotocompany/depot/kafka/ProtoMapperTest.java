package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.runtime.CelEvaluationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProtoMapperTest {

    private StencilClient stencilClient;

    @Before
    public void setUp() {
        stencilClient = StencilClientFactory.getClient();
    }

    @Test
    public void shouldMapDirectField() throws CelEvaluationException, Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"source.order_number\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-123")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("ORD-123",
                result.getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapWithTypeCasting() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"string(source.cancel_reason_id)\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setCancelReasonId(42)
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("42",
                result.getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapWithStringConcatenation() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"\\\"prefix-\\\" + source.order_number\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("123")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("prefix-123",
                result.getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapNestedField() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"string(source.driver_pickup_location.latitude)\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setDriverPickupLocation(TestLocation.newBuilder()
                        .setLatitude(12.34)
                        .build())
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        String resultValue = (String) result.getField(sinkDesc.findFieldByName("order_number"));
        Assert.assertTrue(resultValue.startsWith("12.34"));
    }

    @Test
    public void shouldMapWithTernaryOperator() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"source.order_number == \\\"active\\\" ? \\\"running\\\" : \\\"stopped\\\"\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("active")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("running",
                result.getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test
    public void shouldMapStaticConstant() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"\\\"static-value\\\"\"}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder().build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("static-value",
                result.getField(sinkDesc.findFieldByName("order_number")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnInvalidCelExpression() {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{\"order_number\": \"invalid_syntax %%% bad\"}";
        new ProtoMapper(sourceDesc, sinkDesc, mapping);
    }

    @Test
    public void shouldMapMultipleFields() throws Exception {
        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{"
                + "\"order_number\": \"source.order_number\","
                + "\"order_url\": \"source.order_url\""
                + "}";
        ProtoMapper mapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-1")
                .setOrderUrl("http://example.com/order/1")
                .build();
        DynamicMessage sourceMsg = DynamicMessage.parseFrom(sourceDesc, source.toByteArray());

        DynamicMessage result = mapper.map(sourceMsg);
        Assert.assertEquals("ORD-1",
                result.getField(sinkDesc.findFieldByName("order_number")));
        Assert.assertEquals("http://example.com/order/1",
                result.getField(sinkDesc.findFieldByName("order_url")));
    }
}
