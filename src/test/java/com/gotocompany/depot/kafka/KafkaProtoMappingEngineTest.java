package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingStatus;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.runtime.CelEvaluationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProtoMappingEngineTest {

    private static final String SOURCE_PROTO = "com.gotocompany.depot.TestBookingLogMessage";
    private static final String SINK_MESSAGE_PROTO = "com.gotocompany.depot.TestBookingLogMessage";
    private static final String SINK_KEY_PROTO = "com.gotocompany.depot.TestBookingLogKey";

    @Mock
    private Instrumentation instrumentation;

    private StencilClient stencilClient;
    private DynamicMessage sourceMessage;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);

        TestBookingLogMessage bookingMessage = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-12345")
                .setOrderUrl("http://example.com/orders/12345")
                .setCustomerId("CUST-001")
                .setDriverId("DRV-001")
                .setServiceAreaId("34")
                .setAmountPaidByCash(150.5f)
                .setCancelReasonId(5)
                .setCancelReasonDescription("customer_request")
                .setCustomerEmail("test@example.com")
                .setCustomerName("Test User")
                .setServiceType(TestServiceType.Enum.GO_RIDE)
                .setStatus(TestBookingStatus.Enum.COMPLETED)
                .setDriverPickupLocation(TestLocation.newBuilder()
                        .setName("Pickup Point")
                        .setLatitude(6.2088)
                        .setLongitude(106.8456)
                        .setAddress("Jakarta")
                        .build())
                .setDriverDropoffLocation(TestLocation.newBuilder()
                        .setName("Dropoff Point")
                        .setLatitude(6.9175)
                        .setLongitude(107.6191)
                        .setAddress("Bandung")
                        .build())
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1700000000L).build())
                .setCustomerDynamicSurgeEnabled(true)
                .setCustomerTotalFareWithoutSurge(25000L)
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setTopic("topic-1")
                        .setQos(1)
                        .build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setTopic("topic-2")
                        .setQos(2)
                        .build())
                .build();

        sourceMessage = DynamicMessage.parseFrom(
                stencilClient.get(SOURCE_PROTO),
                bookingMessage.toByteArray()
        );
    }

    private KafkaProtoMappingEngine createEngine(String mappingJson) {
        return createEngine(mappingJson, SINK_MESSAGE_PROTO, SINK_KEY_PROTO);
    }

    private KafkaProtoMappingEngine createEngine(String mappingJson, String sinkMessage, String sinkKey) {
        KafkaSinkConfig config = Mockito.mock(KafkaSinkConfig.class);
        when(config.getSinkConnectorSchemaProtoMessageClass()).thenReturn(SOURCE_PROTO);
        when(config.getSinkKafkaProtoMessage()).thenReturn(sinkMessage);
        when(config.getSinkKafkaProtoKey()).thenReturn(sinkKey);
        when(config.getSinkKafkaProtoMapping()).thenReturn(mappingJson);
        return new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
    }

    // === Direct Field Mapping ===

    @Test
    public void shouldMapDirectStringField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"source.order_number\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("ORD-12345", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
    }

    @Test
    public void shouldMapDirectNumericField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"cancel_reason_id\": \"source.cancel_reason_id\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals(5, result.getField(result.getDescriptorForType().findFieldByName("cancel_reason_id")));
    }

    @Test
    public void shouldMapDirectFloatField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"amount_paid_by_cash\": \"source.amount_paid_by_cash\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        float amount = (float) result.getField(result.getDescriptorForType().findFieldByName("amount_paid_by_cash"));
        Assert.assertEquals(150.5f, amount, 0.01);
    }

    @Test
    public void shouldMapDirectBoolField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"customer_dynamic_surge_enabled\": \"source.customer_dynamic_surge_enabled\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals(true, result.getField(result.getDescriptorForType().findFieldByName("customer_dynamic_surge_enabled")));
    }

    @Test
    public void shouldMapMultipleFields() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"source.order_number\", \"customer_id\": \"source.customer_id\", \"driver_id\": \"source.driver_id\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("ORD-12345", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals("CUST-001", result.getField(result.getDescriptorForType().findFieldByName("customer_id")));
        Assert.assertEquals("DRV-001", result.getField(result.getDescriptorForType().findFieldByName("driver_id")));
    }

    // === String Concatenation ===

    @Test
    public void shouldConcatenateStringWithPrefix() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"\\\"wee\\\" + source.order_number\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("weeORD-12345", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
    }

    @Test
    public void shouldConcatenateMultipleFields() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_url\": \"source.order_number + \\\",\\\" + source.customer_id\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("ORD-12345,CUST-001", result.getField(result.getDescriptorForType().findFieldByName("order_url")));
    }

    // === Explicit Type Casting ===

    @Test
    public void shouldCastIntToString() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"string(source.cancel_reason_id)\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("5", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
    }

    @Test
    public void shouldCastStringConcatWithInt() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"\\\"order-\\\" + string(source.cancel_reason_id)\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("order-5", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
    }

    // === Nested Field Access ===

    @Test
    public void shouldAccessNestedField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"customer_email\": \"source.driver_pickup_location.name\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("Pickup Point", result.getField(result.getDescriptorForType().findFieldByName("customer_email")));
    }

    @Test
    public void shouldAccessDeepNestedField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"customer_email\": \"source.driver_pickup_location.address\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("Jakarta", result.getField(result.getDescriptorForType().findFieldByName("customer_email")));
    }

    // === Ternary Operator ===

    @Test
    public void shouldEvaluateTernaryTrueCondition() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"cancel_reason_description\": \"source.cancel_reason_id > 0 ? \\\"cancelled\\\" : \\\"ok\\\"\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("cancelled", result.getField(result.getDescriptorForType().findFieldByName("cancel_reason_description")));
    }

    @Test
    public void shouldEvaluateTernaryFalseCondition() throws Exception {
        // customer_total_fare_without_surge is 25000, so > 100000 is false
        KafkaProtoMappingEngine engine = createEngine(
                "{\"cancel_reason_description\": \"source.customer_total_fare_without_surge > 100000 ? \\\"expensive\\\" : \\\"cheap\\\"\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("cheap", result.getField(result.getDescriptorForType().findFieldByName("cancel_reason_description")));
    }

    // === Message Type Field Mapping (same type passthrough) ===

    @Test
    public void shouldPassthroughMessageField() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"driver_dropoff_location\": \"source.driver_pickup_location\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Object locationField = result.getField(
                result.getDescriptorForType().findFieldByName("driver_dropoff_location"));
        // The result should be a proto Message (could be DynamicMessage or generated type)
        Assert.assertTrue(locationField instanceof com.google.protobuf.MessageOrBuilder);
        DynamicMessage location;
        if (locationField instanceof DynamicMessage) {
            location = (DynamicMessage) locationField;
        } else {
            location = DynamicMessage.parseFrom(
                    result.getDescriptorForType().findFieldByName("driver_dropoff_location").getMessageType(),
                    ((com.google.protobuf.Message) locationField).toByteArray());
        }
        Assert.assertEquals("Pickup Point",
                location.getField(location.getDescriptorForType().findFieldByName("name")));
        Assert.assertEquals(6.2088,
                (double) location.getField(location.getDescriptorForType().findFieldByName("latitude")), 0.0001);
    }

    // === Field Presence Check (has()) ===

    @Test
    public void shouldCheckFieldPresenceTrue() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"customer_email\": \"has(source.driver_pickup_location) ? source.driver_pickup_location.name : \\\"unknown\\\"\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("Pickup Point", result.getField(result.getDescriptorForType().findFieldByName("customer_email")));
    }

    // === Static/Constant Values ===

    @Test
    public void shouldAssignStaticStringValue() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"service_area_id\": \"\\\"34\\\"\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("34", result.getField(result.getDescriptorForType().findFieldByName("service_area_id")));
    }

    @Test
    public void shouldAssignStaticIntValue() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"cancel_reason_id\": \"42\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals(42, result.getField(result.getDescriptorForType().findFieldByName("cancel_reason_id")));
    }

    @Test
    public void shouldAssignStaticBoolValue() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"customer_dynamic_surge_enabled\": \"true\"}");
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals(true, result.getField(result.getDescriptorForType().findFieldByName("customer_dynamic_surge_enabled")));
    }

    // === Single Mapping for Both Key and Message (D1) ===

    @Test
    public void shouldMapFieldsPresentInKeyProto() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"source.order_number\", \"customer_id\": \"source.customer_id\"}");

        // Map to key — only order_number exists in TestBookingLogKey, customer_id does not
        DynamicMessage keyResult = engine.mapToSinkKey(sourceMessage);
        Assert.assertNotNull(keyResult);
        Assert.assertEquals("ORD-12345",
                keyResult.getField(keyResult.getDescriptorForType().findFieldByName("order_number")));

        // Map to message — both fields exist in TestBookingLogMessage
        DynamicMessage messageResult = engine.mapToSinkMessage(sourceMessage);
        Assert.assertEquals("ORD-12345",
                messageResult.getField(messageResult.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals("CUST-001",
                messageResult.getField(messageResult.getDescriptorForType().findFieldByName("customer_id")));
    }

    @Test
    public void shouldReturnNullKeyWhenNoKeyProtoConfigured() throws Exception {
        KafkaProtoMappingEngine engine = createEngine(
                "{\"order_number\": \"source.order_number\"}", SINK_MESSAGE_PROTO, "");
        DynamicMessage keyResult = engine.mapToSinkKey(sourceMessage);
        Assert.assertNull(keyResult);
    }

    // === Error Scenarios ===

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailFastOnInvalidCelExpression() {
        createEngine("{\"order_number\": \"source.nonexistent_field\"}");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnInvalidMappingJson() {
        createEngine("not a json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnEmptyMapping() {
        createEngine("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnNullMapping() {
        createEngine(null);
    }

    // === Combined Transformation ===

    @Test
    public void shouldHandleCombinedTransformations() throws Exception {
        String mapping = "{"
                + "\"order_number\": \"\\\"wee\\\" + source.order_number\","
                + "\"customer_id\": \"source.customer_id\","
                + "\"cancel_reason_description\": \"source.cancel_reason_id > 0 ? \\\"cancelled\\\" : \\\"ok\\\"\","
                + "\"customer_email\": \"source.driver_pickup_location.name\","
                + "\"service_area_id\": \"\\\"34\\\"\""
                + "}";
        KafkaProtoMappingEngine engine = createEngine(mapping);
        DynamicMessage result = engine.mapToSinkMessage(sourceMessage);

        Assert.assertEquals("weeORD-12345", result.getField(result.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals("CUST-001", result.getField(result.getDescriptorForType().findFieldByName("customer_id")));
        Assert.assertEquals("cancelled", result.getField(result.getDescriptorForType().findFieldByName("cancel_reason_description")));
        Assert.assertEquals("Pickup Point", result.getField(result.getDescriptorForType().findFieldByName("customer_email")));
        Assert.assertEquals("34", result.getField(result.getDescriptorForType().findFieldByName("service_area_id")));
    }
}
