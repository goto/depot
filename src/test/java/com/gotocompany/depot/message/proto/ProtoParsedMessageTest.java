package com.gotocompany.depot.message.proto;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestNestedMessageBQ;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.stencil.Parser;
import com.gotocompany.stencil.StencilClientFactory;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ProtoParsedMessageTest {

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();
    private DynamicMessage dynamicMessage;
    private Instant now;
    private Parser parser;

    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        parser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        now = Instant.now();
        Timestamp createdAt = Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
        TestMessageBQ testMessage = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url")
                .setOrderDetails("order-details")
                .setCreatedAt(createdAt)
                .setStatus(StatusBQ.COMPLETED)
                .setOrderDate(com.google.type.Date.newBuilder().setYear(1996).setMonth(11).setDay(21))
                .build();
        dynamicMessage = parser.parse(testMessage.toByteArray());
    }

    @Test
    public void shouldGetFieldByName() {
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage);
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("order-1", orderNumber);
    }

    @Test
    public void shouldGetComplexFieldByName() throws IOException {
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(123)
                        .setTopic("my-topic").build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage);
        JSONArray f = (JSONArray) protoParsedMessage.getFieldByName("topics");

        Assert.assertEquals(new JSONObject("{\"qos\": 1, \"topic\": \"hellowo/rl/dcom.world.partner\"}").toString(),
                f.get(0).toString());
        Assert.assertEquals(new JSONObject("{\"qos\": 123, \"topic\": \"my-topic\"}").toString(),
                f.get(1).toString());
    }


    @Test
    public void shouldGetStructFromProto() throws IOException {
        TestBookingLogMessage testBookingLogMessage = TestBookingLogMessage.newBuilder()
                .setCustomerName("johndoe")
                .addTopics(TestBookingLogMessage.TopicMetadata.newBuilder()
                        .setQos(1)
                        .setTopic("hellowo/rl/dcom.world.partner").build())
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(10.2).setLongitude(12.01).build())
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestBookingLogMessage.class.getName());
        DynamicMessage bookingLogDynamicMessage = protoParser.parse(testBookingLogMessage.toByteArray());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage);
        Object driverPickupLocation = protoParsedMessage.getFieldByName("driver_pickup_location");
        Assert.assertEquals(PRINTER.print(TestLocation.newBuilder().setLatitude(10.2).setLongitude(12.01).build()), driverPickupLocation.toString());
    }

    @Test
    public void shouldGetRepeatableStructField() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50.02).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(60.1).build()).build())
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("active", Value.newBuilder().setBoolValue(true).build())
                        .putFields("height", Value.newBuilder().setNumberValue(175.9).build()).build())
                .build();

        JSONObject json1 = new JSONObject();
        json1.put("name", "John");
        json1.put("age", 50.02);
        JSONObject json2 = new JSONObject();
        json2.put("name", "John");
        json2.put("age", 60.1);
        JSONObject json3 = new JSONObject();
        json3.put("name", "John");
        json3.put("active", true);
        json3.put("height", 175.9);
        JSONArray expectedValue = new JSONArray();
        expectedValue.put(json1).put(json2).put(json3);

        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        JSONArray attributes = (JSONArray) protoParsedMessage.getFieldByName("attributes");
        assertEquals(expectedValue.toString(), attributes.toString());
    }

    @Test
    public void shouldGetNumberFields() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50L).build()).build())
                .setDiscount(10000012010L)
                .setPrice(10.2f)
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object discount = protoParsedMessage.getFieldByName("discount");
        Assert.assertEquals(10000012010L, discount);
        Object price = protoParsedMessage.getFieldByName("price");
        Assert.assertEquals(Float.valueOf(10.2f).toString(), price.toString());
    }

    @Test
    public void shouldGetRepeatedTimeStamps() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()));
        JSONArray updatedTimeStamps = (JSONArray) protoParsedMessage.getFieldByName("updated_at");
        Assert.assertEquals(2, updatedTimeStamps.length());
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(0));
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(1));
    }


    @Test
    public void shouldGetFieldByNameFromNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        Object orderNumber = protoParsedMessage.getFieldByName("single_message.order_number");
        Assert.assertEquals(message1.getOrderNumber(), orderNumber);
    }

    @Test
    public void shouldReturnInstantField() throws IOException {
        Instant time = Instant.ofEpochSecond(1669160207, 600000000);
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(time);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Assert.assertEquals(time.toString(), protoParsedMessage.getFieldByName("created_at"));
    }

    @Test
    public void shouldReturnDurationFieldInStringFormat() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Object tripDuration = protoParsedMessage.getFieldByName("trip_duration");
        // ProtoFormat Printer returns valid json Value (String with double quotes).
        // Whereas JSONObject, JSONArray returns java types.
        // To return valid value we have to use JSONWriter.
        Assert.assertEquals(
                PRINTER.print(Duration.newBuilder().setSeconds(1).setNanos(TestProtoUtil.TRIP_DURATION_NANOS).build()),
                JSONWriter.valueToString(tripDuration.toString()));
    }

    @Test
    public void shouldReturnMapFieldAsJSONObject() throws IOException {
        TestMessageBQ message1 = TestMessageBQ.newBuilder().putCurrentState("running", "active").build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()));
        Object currentState = protoParsedMessage.getFieldByName("current_state");
        Assert.assertEquals("{\"running\":\"active\"}", currentState.toString());
    }

    @Test
    public void shouldReturnDefaultValueForFieldIfValueIsNotSet() throws IOException {
        TestMessageBQ emptyMessage = TestMessageBQ.newBuilder().build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(emptyMessage.toByteArray()));
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("", orderNumber);
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotPresentInProto() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("single_message.order_id"));
        Assert.assertEquals("Invalid field config : single_message.order_id", exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionIfColumnIsNotNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()));
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("nested_id.order_id"));
        Assert.assertEquals("Invalid field config : nested_id.order_id", exception.getMessage());
    }


    @Test
    public void shouldThrowExceptionIfFieldIsEmpty() {
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName(""));
        Assert.assertEquals("Invalid field config : name can not be empty", exception.getMessage());
    }

    @Test
    public void shouldReturnRepeatedDurations() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()));
        JSONArray intervals = (JSONArray) protoParsedMessage.getFieldByName("intervals");
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(12).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(0)));
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(15).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(1)));
    }

    @Test
    public void shouldReturnRepeatedString() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        JSONArray listValues = (JSONArray) protoParsedMessage.getFieldByName("list_values");
        Assert.assertEquals("test1", listValues.get(0));
        Assert.assertEquals("test2", listValues.get(1));
        Assert.assertEquals("test3", listValues.get(2));
    }

    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object value = protoParsedMessage.getFieldByName("list_values[1]");
        Assert.assertEquals("test2", value.toString());
    }

    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedMessageField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("123"))
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("456"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Object value = protoParsedMessage.getFieldByName("list_message_values[0]");
        Assert.assertEquals("{\"order_number\":\"123\"}", value.toString());
    }

    @Test
    public void shouldIncludeDefaultEnumFieldsOnGetFields() throws IOException {
        TestTypesMessage message = TestTypesMessage.getDefaultInstance();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<SchemaField, Object> fields = protoParsedMessage.getFields();
        Optional<SchemaField> enumValue = fields.keySet().stream().filter(f -> f.getName().equals("enum_value")).findFirst();
        Assert.assertTrue(enumValue.isPresent());
        Assert.assertEquals("CATEGORY_1", fields.get(enumValue.get()));
    }

    private Object getFieldsValue(Map<SchemaField, Object> fields, String name) {
        return fields.entrySet().stream().filter(f -> f.getKey().getName().equals(name)).findFirst().get().getValue();
    }


    @Test
    public void shouldReturnParsedMessageIfFieldValueIsOfTypeMessage() throws IOException {
        TestMessage msg = TestMessage.newBuilder().setOrderNumber("order-number").build();
        TestTypesMessage message = TestTypesMessage.newBuilder().setMessageValue(msg).addAllListMessageValues(Arrays.asList(msg, msg)).build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<SchemaField, Object> fields = protoParsedMessage.getFields();
        Assert.assertTrue(getFieldsValue(fields, "message_value") instanceof ProtoParsedMessage);
        List<?> listValue = (List<?>) getFieldsValue(fields, "list_message_values");
        Assert.assertEquals(2, listValue.size());
        Assert.assertTrue(listValue.get(0) instanceof ProtoParsedMessage);
    }

    private boolean isFieldPresent(Map<SchemaField, Object> fields, String name) {
        return fields.entrySet().stream().anyMatch(f -> f.getKey().getName().equals(name));
    }

    @Test
    public void shouldIncludeDefaultValuesForMessage() throws IOException {
        TestTypesMessage  message = TestTypesMessage.getDefaultInstance();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<SchemaField, Object> fields = protoParsedMessage.getFields();
        Assert.assertEquals(Float.valueOf("0.0"), getFieldsValue(fields, "float_value"));
        Assert.assertEquals(Double.parseDouble("0.0"), getFieldsValue(fields, "double_value"));
        Assert.assertEquals(0, getFieldsValue(fields, "int32_value"));
        Assert.assertEquals(0L, getFieldsValue(fields, "int64_value"));
        Assert.assertEquals(0, getFieldsValue(fields, "uint32_value"));
        Assert.assertEquals(0L, getFieldsValue(fields, "uint64_value"));
        Assert.assertEquals(0, getFieldsValue(fields, "fixed32_value"));
        Assert.assertEquals(0L, getFieldsValue(fields, "fixed64_value"));
        Assert.assertEquals(0, getFieldsValue(fields, "sfixed32_value"));
        Assert.assertEquals(0L, getFieldsValue(fields, "sfixed64_value"));
        Assert.assertEquals(0, getFieldsValue(fields, "sint32_value"));
        Assert.assertEquals(0L, getFieldsValue(fields, "sint64_value"));
        Assert.assertEquals("CATEGORY_1", getFieldsValue(fields, "enum_value"));
        Assert.assertEquals("", ((ByteString) getFieldsValue(fields, "bytes_value")).toString(StandardCharsets.UTF_8));
        Assert.assertEquals(false, getFieldsValue(fields, "bool_value"));
        // these fields shouldn't be included
        Assert.assertFalse(isFieldPresent(fields, "string_value"));
        Assert.assertFalse(isFieldPresent(fields, "message_value"));
        Assert.assertFalse(isFieldPresent(fields, "list_values"));
        Assert.assertFalse(isFieldPresent(fields, "list_message_values"));
        Assert.assertFalse(isFieldPresent(fields, "timestamp_value"));
    }

    @Test
    public void shouldReturnJsonObjectWithPreservedFieldNames() throws InvalidProtocolBufferException {
        JSONObject jsonObject = new JSONObject(""
                + "{\"string_value\": \"test-string\","
                + " \"float_value\": 10.0, "
                + "\"message_value\" : {\"order_number\" : \"order-1\", \"order_details\" : \"order-details-1\"}"
                + "}");
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setFloatValue(10.0f)
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_PRESERVE_PROTO_FIELD_NAMES_ENABLE", "true");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, sinkConfig);
        assertEquals(jsonObject.toString(), protoParsedMessage.toJson(config).toString());
    }

    @Test
    public void shouldReturnJsonObjectWithNoPreservedFieldNames() throws InvalidProtocolBufferException {
        JSONObject jsonObject = new JSONObject(""
                + "{\"stringValue\": \"test-string\","
                + " \"floatValue\": 10.0, "
                + "\"messageValue\" : {\"orderNumber\" : \"order-1\", \"orderDetails\" : \"order-details-1\"}"
                + "}");
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setFloatValue(10.0f)
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_PRESERVE_PROTO_FIELD_NAMES_ENABLE", "false");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, sinkConfig);
        assertEquals(jsonObject.toString(), protoParsedMessage.toJson(config).toString());
    }

    @Test
    public void shouldThrowExceptionForInvalidProtoMessage() throws InvalidProtocolBufferException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setTimestampValue(Timestamp.newBuilder().setSeconds(-99999999999999L).setNanos(0).build())
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_PRESERVE_PROTO_FIELD_NAMES_ENABLE", "false");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, sinkConfig);
        assertThrows(DeserializerException.class, () -> protoParsedMessage.toJson(config));
    }

    @Test
    public void shouldThrowExceptionForInvalidType() throws InvalidProtocolBufferException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setAnyValue(Any.newBuilder().setTypeUrl("type-url").setValue(ByteString.copyFromUtf8("test-string")).build())
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()));
        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put("SINK_CONNECTOR_SCHEMA_PROTO_PRESERVE_PROTO_FIELD_NAMES_ENABLE", "false");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, sinkConfig);
        assertThrows(DeserializerException.class, () -> protoParsedMessage.toJson(config));
    }
}
