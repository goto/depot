package com.gotocompany.depot.kafka.mapping;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestKafkaOutputKey;
import com.gotocompany.depot.TestKafkaOutputMessage;
import com.gotocompany.depot.TestKafkaServiceType;
import com.gotocompany.depot.TestKafkaSourceAddress;
import com.gotocompany.depot.TestKafkaSourceLocation;
import com.gotocompany.depot.TestKafkaSourceMessage;
import com.gotocompany.depot.TestKafkaSourceStatus;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.ProtoMappingException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ProtoMappingFunctionFactoryTest {

    private final Descriptors.Descriptor sourceDescriptor = TestKafkaSourceMessage.getDescriptor();
    private final Descriptors.Descriptor valueDescriptor = TestKafkaOutputMessage.getDescriptor();
    private final Descriptors.Descriptor keyDescriptor = TestKafkaOutputKey.getDescriptor();
    private ProtoMappingFunctionFactory mappingFunctionFactory;
    private TestKafkaSourceMessage sourceMessage;

    @Before
    public void setup() {
        mappingFunctionFactory = new ProtoMappingFunctionFactory();
        sourceMessage = TestKafkaSourceMessage.newBuilder()
                .setOrderNumber(93L)
                .setAccountGoId("user-1")
                .setStatus(TestKafkaSourceStatus.newBuilder().setLastStatus("approved").build())
                .addOrderList(11L).addOrderList(22L).addOrderList(33L)
                .setOrderLocation(TestKafkaSourceLocation.newBuilder().setLatitude(1.5).setLongitude(2.5).build())
                .setCurrentStatus("active")
                .setAddress(TestKafkaSourceAddress.newBuilder().setCity("jakarta").build())
                .setPickupLocation(TestKafkaSourceLocation.newBuilder().setLatitude(3.5).setLongitude(4.5).build())
                .setAmount(42.7)
                .setOrderType(5)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1700000000L).setNanos(123).build())
                .setServiceType(TestKafkaServiceType.Enum.GO_FOOD)
                .setPayload(ByteString.copyFromUtf8("abc"))
                .putLabels("k1", "v1")
                .setRetryCount(7)
                .setTotalCount(99L)
                .setFloatAmount(1.25f)
                .addTags("a").addTags("b")
                .build();
    }

    private Map<String, String> getFullProtoMapping() {
        Map<String, String> protoMapping = new LinkedHashMap<>();
        protoMapping.put("order_id", "\"wee\" + string(source.order_number)");
        protoMapping.put("user_id", "source.account_go_id");
        protoMapping.put("approval_status", "source.status.last_status");
        protoMapping.put("order_lat", "source.order_location.latitude");
        protoMapping.put("order_lng", "source.order_location.longitude");
        protoMapping.put("primary_order_id", "source.order_list[2]");
        protoMapping.put("service_area_id", "34");
        protoMapping.put("order_labels", "[2343, 4434, 6454]");
        protoMapping.put("status", "source.current_status == \"active\" ? \"running\" : \"stopped\"");
        protoMapping.put("city", "has(source.address) ? source.address.city : \"unknown\"");
        protoMapping.put("origin", "source.pickup_location");
        protoMapping.put("sink_origin", "com.gotocompany.depot.TestKafkaSinkLocation{lat: source.pickup_location.latitude, lng: source.pickup_location.longitude}");
        protoMapping.put("is_valid", "source.amount > 0.0 && source.current_status != \"cancelled\"");
        protoMapping.put("order_amount", "int(source.amount)");
        protoMapping.put("event_timestamp", "source.event_timestamp");
        protoMapping.put("service_type", "source.service_type");
        protoMapping.put("payload", "source.payload");
        protoMapping.put("labels", "source.labels");
        protoMapping.put("retry_count", "source.retry_count");
        protoMapping.put("total_count", "source.total_count");
        protoMapping.put("float_amount", "source.float_amount");
        protoMapping.put("tags", "source.tags");
        protoMapping.put("order_note", "\"note-\" + source.account_go_id");
        protoMapping.put("order_number", "source.order_number");
        return protoMapping;
    }

    @Test
    public void shouldMapAllSupportedTransformationFeatures() throws InvalidProtocolBufferException {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, keyDescriptor, getFullProtoMapping());
        DynamicMessage mappedValue = mappingFunction.mapValue(sourceMessage);
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappedValue.toByteArray());
        assertEquals("wee93", outputMessage.getOrderId());
        assertEquals("user-1", outputMessage.getUserId());
        assertEquals("approved", outputMessage.getApprovalStatus());
        assertEquals(1.5, outputMessage.getOrderLat(), 0.000001);
        assertEquals(2.5, outputMessage.getOrderLng(), 0.000001);
        assertEquals(33L, outputMessage.getPrimaryOrderId());
        assertEquals(34L, outputMessage.getServiceAreaId());
        assertEquals(3, outputMessage.getOrderLabelsCount());
        assertEquals(2343L, outputMessage.getOrderLabels(0));
        assertEquals(4434L, outputMessage.getOrderLabels(1));
        assertEquals(6454L, outputMessage.getOrderLabels(2));
        assertEquals("running", outputMessage.getStatus());
        assertEquals("jakarta", outputMessage.getCity());
        assertEquals(3.5, outputMessage.getOrigin().getLatitude(), 0.000001);
        assertEquals(4.5, outputMessage.getOrigin().getLongitude(), 0.000001);
        assertEquals(3.5, outputMessage.getSinkOrigin().getLat(), 0.000001);
        assertEquals(4.5, outputMessage.getSinkOrigin().getLng(), 0.000001);
        assertTrue(outputMessage.getIsValid());
        assertEquals(42, outputMessage.getOrderAmount());
        assertEquals(1700000000L, outputMessage.getEventTimestamp().getSeconds());
        assertEquals(123, outputMessage.getEventTimestamp().getNanos());
        assertEquals(TestKafkaServiceType.Enum.GO_FOOD, outputMessage.getServiceType());
        assertEquals(ByteString.copyFromUtf8("abc"), outputMessage.getPayload());
        assertEquals("v1", outputMessage.getLabelsOrThrow("k1"));
        assertEquals(7, outputMessage.getRetryCount());
        assertEquals(99L, outputMessage.getTotalCount());
        assertEquals(1.25f, outputMessage.getFloatAmount(), 0.000001f);
        assertEquals(2, outputMessage.getTagsCount());
        assertEquals("a", outputMessage.getTags(0));
        assertEquals("b", outputMessage.getTags(1));
        assertEquals("note-user-1", outputMessage.getOrderNote().getValue());
    }

    @Test
    public void shouldMapFieldsToBothKeyAndValueProtos() throws InvalidProtocolBufferException {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, keyDescriptor, getFullProtoMapping());
        assertTrue(mappingFunction.hasKeyMapping());
        TestKafkaOutputKey outputKey = TestKafkaOutputKey.parseFrom(mappingFunction.mapKey(sourceMessage).toByteArray());
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals("wee93", outputKey.getOrderId());
        assertEquals(93L, outputKey.getOrderNumber());
        assertEquals("wee93", outputMessage.getOrderId());
    }

    @Test
    public void shouldUseTernaryFallbackWhenNestedFieldIsNotSet() throws InvalidProtocolBufferException {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("city", "has(source.address) ? source.address.city : \"unknown\""));
        TestKafkaSourceMessage sourceWithoutAddress = TestKafkaSourceMessage.newBuilder().setOrderNumber(1L).build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceWithoutAddress).toByteArray());
        assertEquals("unknown", outputMessage.getCity());
    }

    @Test
    public void shouldReturnDefaultValuesForUnmappedFields() throws InvalidProtocolBufferException {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("user_id", "source.account_go_id"));
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals("user-1", outputMessage.getUserId());
        assertEquals("", outputMessage.getOrderId());
        assertEquals(0L, outputMessage.getServiceAreaId());
        Assert.assertFalse(mappingFunction.hasKeyMapping());
    }

    @Test
    public void shouldMapEnumFieldFromStringConstant() throws InvalidProtocolBufferException {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("service_type", "\"GO_RIDE\""));
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals(TestKafkaServiceType.Enum.GO_RIDE, outputMessage.getServiceType());
    }

    @Test
    public void shouldThrowMappingExceptionForRuntimeEvaluationErrors() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("primary_order_id", "source.order_list[5]"));
        ProtoMappingException exception = assertThrows(ProtoMappingException.class, () -> mappingFunction.mapValue(sourceMessage));
        assertTrue(exception.getMessage().contains("primary_order_id"));
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenKeyMappingIsNotConfigured() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("user_id", "source.account_go_id"));
        assertThrows(IllegalStateException.class, () -> mappingFunction.mapKey(sourceMessage));
    }

    @Test
    public void shouldFailForEmptyMapping() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor, Collections.emptyMap()));
        assertEquals("SINK_KAFKA_PROTO_MAPPING should contain at least one field mapping", exception.getMessage());
    }

    @Test
    public void shouldFailForNullMapping() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor, null));
        assertEquals("SINK_KAFKA_PROTO_MAPPING should contain at least one field mapping", exception.getMessage());
    }

    @Test
    public void shouldFailForFieldsNotPresentInOutputProtos() {
        Map<String, String> protoMapping = new LinkedHashMap<>();
        protoMapping.put("unknown_field", "source.account_go_id");
        protoMapping.put("user_id", "source.account_go_id");
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor, protoMapping));
        assertTrue(exception.getMessage().contains("unknown_field"));
        Assert.assertFalse(exception.getMessage().contains("user_id"));
    }

    @Test
    public void shouldFailForInvalidExpressionSyntax() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("user_id", "source.account_go_id +")));
        assertTrue(exception.getMessage().contains("user_id"));
    }

    @Test
    public void shouldFailForUnknownSourceField() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("user_id", "source.not_a_field")));
        assertTrue(exception.getMessage().contains("not_a_field"));
    }

    @Test
    public void shouldFailForTypeMismatchBetweenExpressionAndOutputField() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("user_id", "source.order_number")));
        assertTrue(exception.getMessage().contains("user_id"));
    }

    @Test
    public void shouldFailForMessageTypeMismatch() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("sink_origin", "source.pickup_location")));
        assertTrue(exception.getMessage().contains("sink_origin"));
    }

    @Test
    public void shouldEvaluateAgainstRefreshedSourceDescriptorInstance() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("user_id", "source.account_go_id"));
        DynamicMessage dynamicSource = DynamicMessage.parseFrom(sourceDescriptor, sourceMessage.toByteArray());
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(dynamicSource).toByteArray());
        assertEquals("user-1", outputMessage.getUserId());
        assertNotNull(outputMessage);
    }

    @Test
    public void shouldMapWithIdenticalSourceAndSinkSchemas() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, sourceDescriptor, null,
                Collections.singletonMap("order_number", "source.order_number"));
        TestKafkaSourceMessage outputMessage = TestKafkaSourceMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals(93L, outputMessage.getOrderNumber());
    }

    @Test
    public void shouldMapWhenSinkDescriptorsComeFromDifferentRegistryInstance() throws Exception {
        Descriptors.FileDescriptor rebuiltFile = Descriptors.FileDescriptor.buildFrom(
                com.gotocompany.depot.TestKafkaSinkProto.getDescriptor().toProto(),
                new Descriptors.FileDescriptor[]{
                        com.google.protobuf.TimestampProto.getDescriptor(),
                        com.google.protobuf.WrappersProto.getDescriptor()});
        Descriptors.Descriptor rebuiltValueDescriptor = rebuiltFile.findMessageTypeByName("TestKafkaOutputMessage");
        Map<String, String> protoMapping = new LinkedHashMap<>();
        protoMapping.put("user_id", "source.account_go_id");
        protoMapping.put("origin", "source.pickup_location");
        protoMapping.put("sink_origin", "com.gotocompany.depot.TestKafkaSinkLocation{lat: source.pickup_location.latitude, lng: source.pickup_location.longitude}");
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, rebuiltValueDescriptor, null, protoMapping);
        DynamicMessage mappedValue = mappingFunction.mapValue(sourceMessage);
        assertEquals(rebuiltValueDescriptor, mappedValue.getDescriptorForType());
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappedValue.toByteArray());
        assertEquals("user-1", outputMessage.getUserId());
        assertEquals(3.5, outputMessage.getOrigin().getLatitude(), 0.000001);
        assertEquals(3.5, outputMessage.getSinkOrigin().getLat(), 0.000001);
        assertEquals(4.5, outputMessage.getSinkOrigin().getLng(), 0.000001);
    }

    @Test
    public void shouldConstructWellKnownTimestampMessage() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("event_timestamp", "google.protobuf.Timestamp{seconds: 1700000001, nanos: 5}"));
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals(1700000001L, outputMessage.getEventTimestamp().getSeconds());
        assertEquals(5, outputMessage.getEventTimestamp().getNanos());
    }

    @Test
    public void shouldRoundTripMaxUnsignedLongThroughMapping() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("total_count", "source.total_count"));
        TestKafkaSourceMessage unsignedSource = TestKafkaSourceMessage.newBuilder().setTotalCount(-1L).build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(unsignedSource).toByteArray());
        assertEquals(-1L, outputMessage.getTotalCount());
    }

    @Test
    public void shouldSkipFieldForNullLiteralExpression() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("service_type", "null"));
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(sourceMessage).toByteArray());
        assertEquals(TestKafkaServiceType.Enum.UNKNOWN, outputMessage.getServiceType());
    }

    @Test
    public void shouldThrowMappingExceptionForDivisionByZero() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("primary_order_id", "source.order_number / 0"));
        ProtoMappingException exception = assertThrows(ProtoMappingException.class, () -> mappingFunction.mapValue(sourceMessage));
        assertTrue(exception.getMessage().contains("primary_order_id"));
    }

    @Test
    public void shouldThrowMappingExceptionForNegativeListIndex() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("primary_order_id", "source.order_list[-1]"));
        assertThrows(ProtoMappingException.class, () -> mappingFunction.mapValue(sourceMessage));
    }

    @Test
    public void shouldThrowMappingExceptionForIntOverflowAtRuntime() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("order_amount", "int(source.amount)"));
        TestKafkaSourceMessage hugeAmountSource = TestKafkaSourceMessage.newBuilder().setAmount(1e18).build();
        ProtoMappingException exception = assertThrows(ProtoMappingException.class, () -> mappingFunction.mapValue(hugeAmountSource));
        assertTrue(exception.getMessage().contains("out of range"));
    }

    @Test
    public void shouldThrowMappingExceptionForUnknownEnumNumberAtRuntime() {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("service_type", "99"));
        ProtoMappingException exception = assertThrows(ProtoMappingException.class, () -> mappingFunction.mapValue(sourceMessage));
        assertTrue(exception.getMessage().contains("not a valid enum value"));
    }

    @Test
    public void shouldFailForConcatenationWithoutExplicitCast() {
        ConfigurationException exception = assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("order_id", "\"wee\" + source.order_number")));
        assertTrue(exception.getMessage().contains("order_id"));
    }

    @Test
    public void shouldFailForListLiteralOnSingularField() {
        assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("service_area_id", "[1, 2, 3]")));
    }

    @Test
    public void shouldFailForListLiteralWithMismatchedElementTypeOnRepeatedField() {
        assertThrows(ConfigurationException.class,
                () -> mappingFunctionFactory.create(sourceDescriptor, valueDescriptor, keyDescriptor,
                        Collections.singletonMap("order_labels", "[\"one\", \"two\"]")));
    }

    @Test
    public void shouldBuildEmptyKeyMessageWhenNoMappedFieldsExistInKeyProto() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, keyDescriptor,
                Collections.singletonMap("user_id", "source.account_go_id"));
        assertTrue(mappingFunction.hasKeyMapping());
        DynamicMessage mappedKey = mappingFunction.mapKey(sourceMessage);
        assertEquals(0, mappedKey.toByteArray().length);
        assertEquals(TestKafkaOutputKey.getDefaultInstance(), TestKafkaOutputKey.parseFrom(mappedKey.toByteArray()));
    }

    @Test
    public void shouldMapRepeatedStringFieldPassThrough() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("tags", "source.tags"));
        TestKafkaSourceMessage emptyTagsSource = TestKafkaSourceMessage.newBuilder().build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(emptyTagsSource).toByteArray());
        assertEquals(0, outputMessage.getTagsCount());
    }

    @Test
    public void shouldMapEmptyMapFieldPassThrough() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("labels", "source.labels"));
        TestKafkaSourceMessage emptyLabelsSource = TestKafkaSourceMessage.newBuilder().build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(emptyLabelsSource).toByteArray());
        assertEquals(0, outputMessage.getLabelsCount());
    }

    @Test
    public void shouldMapUnsetNestedMessageToDefaultInstance() throws Exception {
        ProtoMappingFunction mappingFunction = mappingFunctionFactory.create(
                sourceDescriptor, valueDescriptor, null,
                Collections.singletonMap("origin", "source.pickup_location"));
        TestKafkaSourceMessage emptySource = TestKafkaSourceMessage.newBuilder().build();
        TestKafkaOutputMessage outputMessage = TestKafkaOutputMessage.parseFrom(mappingFunction.mapValue(emptySource).toByteArray());
        assertEquals(0.0, outputMessage.getOrigin().getLatitude(), 0.000001);
        assertEquals(0.0, outputMessage.getOrigin().getLongitude(), 0.000001);
    }
}
