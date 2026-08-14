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
import com.jayway.jsonpath.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProtoParsedMessage}, the Protobuf-backed parsed-message implementation that
 * exposes field lookups and JSON conversion over a {@code DynamicMessage}.
 *
 * <p>The {@link #setUp()} fixture builds a Stencil {@link Parser} for {@code TestMessageBQ}, a mocked
 * {@link SinkConfig} (with default-field-value population disabled) and a JsonPath
 * {@link Configuration} backed by a {@link ProtoJsonProvider}, then parses a sample message into the
 * shared {@link #dynamicMessage}. Individual tests construct further protos through the Stencil parser
 * and {@link TestProtoUtil}, then assert {@link ProtoParsedMessage#getFieldByName(String)} for scalar,
 * nested, repeated, struct, map, timestamp and duration fields, the field map returned by
 * {@link ProtoParsedMessage#getFields()} (including default values), and JSON conversion via
 * {@link ProtoParsedMessage#toJson()} together with its {@link DeserializerException} error paths.</p>
 */
public class ProtoParsedMessageTest {

    /** JSON printer preserving proto field names and omitting insignificant whitespace for expectations. */
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();
    /** Shared dynamic message parsed from a sample {@code TestMessageBQ}. */
    private DynamicMessage dynamicMessage;
    /** Timestamp captured at setup and reused for created-at and updated-at fields. */
    private Instant now;
    /** Stencil parser for {@code TestMessageBQ} used to build the shared message. */
    private Parser parser;
    /** Mocked sink config controlling default-field-value population in the JSON provider. */
    @Mock
    private SinkConfig sinkConfig;
    /** JsonPath configuration backed by a {@link ProtoJsonProvider} for field lookups. */
    private Configuration jsonPathConfig;


    /**
     * Builds the parser, mocked config, JSON provider configuration and sample message before each
     * test.
     *
     * <p>Obtains a Stencil {@link Parser} for {@code TestMessageBQ}, captures {@link #now}, mocks the
     * {@link SinkConfig} so default field values are not populated, builds the JsonPath
     * {@link Configuration} from a {@link ProtoJsonProvider}, and parses a sample {@code TestMessageBQ}
     * into {@link #dynamicMessage}.</p>
     *
     * @throws IOException if building the parser or message fails
     * @throws Descriptors.DescriptorValidationException if the descriptor is invalid
     */
    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        parser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        now = Instant.now();
        sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkDefaultFieldValueEnable()).thenReturn(false);
        jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();

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

    /**
     * Verifies that a top-level scalar field is read by name.
     *
     * <p>Given the shared message, when {@link ProtoParsedMessage#getFieldByName(String)} is queried
     * for {@code "order_number"}, then it returns {@code "order-1"}.</p>
     */
    @Test
    public void shouldGetFieldByName() {

        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("order-1", orderNumber);
    }

    /**
     * Verifies that a repeated message field is returned as a JSON array of objects.
     *
     * <p>Given a {@link TestBookingLogMessage} with two {@code topics} entries, when
     * {@link ProtoParsedMessage#getFieldByName(String)} is queried for {@code "topics"}, then it
     * returns a {@link JSONArray} whose elements carry the expected {@code qos} and {@code topic}
     * values.</p>
     *
     * @throws IOException if parsing fails
     */
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
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage, jsonPathConfig);
        JSONArray f = (JSONArray) protoParsedMessage.getFieldByName("topics");

        Assert.assertEquals(new JSONObject("{\"qos\": 1, \"topic\": \"hellowo/rl/dcom.world.partner\"}").toString(),
                f.get(0).toString());
        Assert.assertEquals(new JSONObject("{\"qos\": 123, \"topic\": \"my-topic\"}").toString(),
                f.get(1).toString());
    }


    /**
     * Verifies that a nested message field is rendered as its JSON form.
     *
     * <p>Given a booking-log message with a {@code driver_pickup_location}, when
     * {@link ProtoParsedMessage#getFieldByName(String)} is queried for it, then the value equals the
     * JSON printed for the corresponding {@link TestLocation}.</p>
     *
     * @throws IOException if parsing fails
     */
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
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(bookingLogDynamicMessage, jsonPathConfig);
        Object driverPickupLocation = protoParsedMessage.getFieldByName("driver_pickup_location");
        Assert.assertEquals(PRINTER.print(TestLocation.newBuilder().setLatitude(10.2).setLongitude(12.01).build()), driverPickupLocation.toString());
    }

    /**
     * Verifies that a repeated {@code Struct} field is returned as a JSON array.
     *
     * <p>Given a message with three {@code attributes} structs, when
     * {@link ProtoParsedMessage#getFieldByName(String)} is queried for {@code "attributes"}, then it
     * returns a {@link JSONArray} equal to the expected array of objects.</p>
     *
     * @throws IOException if parsing fails
     */
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
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        JSONArray attributes = (JSONArray) protoParsedMessage.getFieldByName("attributes");
        assertEquals(expectedValue.toString(), attributes.toString());
    }

    /**
     * Verifies that numeric fields are returned with their numeric types preserved.
     *
     * <p>Given a message with a {@code discount} ({@code int64}) and a {@code price} ({@code float}),
     * when each is read by name, then the discount equals the original {@code long} and the price
     * matches the original {@code float} value.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldGetNumberFields() throws IOException {
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .addAttributes(Struct.newBuilder().putFields("name", Value.newBuilder().setStringValue("John").build())
                        .putFields("age", Value.newBuilder().setNumberValue(50L).build()).build())
                .setDiscount(10000012010L)
                .setPrice(10.2f)
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        Object discount = protoParsedMessage.getFieldByName("discount");
        Assert.assertEquals(10000012010L, discount);
        Object price = protoParsedMessage.getFieldByName("price");
        Assert.assertEquals(Float.valueOf(10.2f).toString(), price.toString());
    }

    /**
     * Verifies that a repeated timestamp field is returned as a JSON array of instants.
     *
     * <p>Given a generated message with two {@code updated_at} timestamps, when
     * {@link ProtoParsedMessage#getFieldByName(String)} is queried for {@code "updated_at"}, then it
     * returns a two-element {@link JSONArray} whose values equal {@link #now} as an ISO string.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldGetRepeatedTimeStamps() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()), jsonPathConfig);
        JSONArray updatedTimeStamps = (JSONArray) protoParsedMessage.getFieldByName("updated_at");
        Assert.assertEquals(2, updatedTimeStamps.length());
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(0));
        Assert.assertEquals(now.toString(), updatedTimeStamps.get(1));
    }


    /**
     * Verifies that nested fields are addressable with a dotted path.
     *
     * <p>Given a {@link TestNestedMessageBQ} wrapping a generated message, when the fields
     * {@code "nested_id"} and {@code "single_message.order_number"} are read, then they resolve to the
     * nested id and the inner message's order number.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldGetFieldByNameFromNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()), jsonPathConfig);
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        Object orderNumber = protoParsedMessage.getFieldByName("single_message.order_number");
        Assert.assertEquals(message1.getOrderNumber(), orderNumber);
    }

    /**
     * Verifies that a timestamp field is returned as an ISO instant string.
     *
     * <p>Given a generated message whose {@code created_at} is a known instant, when it is read by
     * name, then the value equals that instant's string form.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnInstantField() throws IOException {
        Instant time = Instant.ofEpochSecond(1669160207, 600000000);
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(time);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()), jsonPathConfig);
        Assert.assertEquals(time.toString(), protoParsedMessage.getFieldByName("created_at"));
    }

    /**
     * Verifies that a duration field is returned in its string form.
     *
     * <p>Given a generated message with a {@code trip_duration}, when it is read by name, then its
     * value matches the JSON printed for the corresponding {@code Duration}.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnDurationFieldInStringFormat() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()), jsonPathConfig);
        Object tripDuration = protoParsedMessage.getFieldByName("trip_duration");
        // ProtoFormat Printer returns valid json Value (String with double quotes).
        // Whereas JSONObject, JSONArray returns java types.
        // To return valid value we have to use JSONWriter.
        Assert.assertEquals(
                PRINTER.print(Duration.newBuilder().setSeconds(1).setNanos(TestProtoUtil.TRIP_DURATION_NANOS).build()),
                JSONWriter.valueToString(tripDuration.toString()));
    }

    /**
     * Verifies that a proto map field is returned as a JSON object.
     *
     * <p>Given a message with a {@code current_state} map entry, when it is read by name, then the
     * value renders as {@code {"running":"active"}}.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnMapFieldAsJSONObject() throws IOException {
        TestMessageBQ message1 = TestMessageBQ.newBuilder().putCurrentState("running", "active").build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(message1.toByteArray()), jsonPathConfig);
        Object currentState = protoParsedMessage.getFieldByName("current_state");
        Assert.assertEquals("{\"running\":\"active\"}", currentState.toString());
    }

    /**
     * Verifies that an unset scalar field returns its proto default value.
     *
     * <p>Given an empty {@link TestMessageBQ}, when {@code "order_number"} is read by name, then it
     * returns the empty string.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnDefaultValueForFieldIfValueIsNotSet() throws IOException {
        TestMessageBQ emptyMessage = TestMessageBQ.newBuilder().build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(parser.parse(emptyMessage.toByteArray()), jsonPathConfig);
        Object orderNumber = protoParsedMessage.getFieldByName("order_number");
        Assert.assertEquals("", orderNumber);
    }

    /**
     * Verifies that reading a nested field that does not exist is rejected.
     *
     * <p>Given a nested message, when {@code "single_message.order_id"} is read by name, then an
     * {@link IllegalArgumentException} with the message
     * {@code "Invalid field config : single_message.order_id"} is thrown.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldThrowExceptionIfColumnIsNotPresentInProto() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()), jsonPathConfig);
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("single_message.order_id"));
        Assert.assertEquals("Invalid field config : single_message.order_id", exception.getMessage());
    }

    /**
     * Verifies that treating a scalar field as nested is rejected.
     *
     * <p>Given a nested message, when {@code "nested_id.order_id"} is read by name (with
     * {@code nested_id} being a scalar), then an {@link IllegalArgumentException} with the message
     * {@code "Invalid field config : nested_id.order_id"} is thrown.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldThrowExceptionIfColumnIsNotNested() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestNestedMessageBQ.class.getName());
        TestNestedMessageBQ nestedMessage = TestNestedMessageBQ.newBuilder().setNestedId("test").setSingleMessage(message1).build();
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(nestedMessage.toByteArray()), jsonPathConfig);
        Object nestedId = protoParsedMessage.getFieldByName("nested_id");
        Assert.assertEquals("test", nestedId);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName("nested_id.order_id"));
        Assert.assertEquals("Invalid field config : nested_id.order_id", exception.getMessage());
    }


    /**
     * Verifies that an empty field name is rejected.
     *
     * <p>Given the shared message, when {@link ProtoParsedMessage#getFieldByName(String)} is queried
     * with an empty string, then an {@link IllegalArgumentException} with the message
     * {@code "Invalid field config : name can not be empty"} is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionIfFieldIsEmpty() {
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> protoParsedMessage.getFieldByName(""));
        Assert.assertEquals("Invalid field config : name can not be empty", exception.getMessage());
    }

    /**
     * Verifies that a repeated duration field is returned as a JSON array of durations.
     *
     * <p>Given a generated message with two {@code intervals}, when {@code "intervals"} is read by
     * name, then the returned {@link JSONArray} elements match the JSON printed for the expected
     * {@code Duration} values.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnRepeatedDurations() throws IOException {
        TestMessageBQ message1 = TestProtoUtil.generateTestMessage(now);
        Parser protoParser = StencilClientFactory.getClient().getParser(TestMessageBQ.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message1.toByteArray()), jsonPathConfig);
        JSONArray intervals = (JSONArray) protoParsedMessage.getFieldByName("intervals");
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(12).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(0)));
        Assert.assertEquals(PRINTER.print(Duration.newBuilder().setSeconds(15).setNanos(1000).build()), JSONWriter.valueToString(intervals.get(1)));
    }

    /**
     * Verifies that a repeated string field is returned as a JSON array of strings.
     *
     * <p>Given a {@link TestTypesMessage} with three {@code list_values}, when {@code "list_values"}
     * is read by name, then the returned {@link JSONArray} holds the three strings in order.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnRepeatedString() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        JSONArray listValues = (JSONArray) protoParsedMessage.getFieldByName("list_values");
        Assert.assertEquals("test1", listValues.get(0));
        Assert.assertEquals("test2", listValues.get(1));
        Assert.assertEquals("test3", listValues.get(2));
    }

    /**
     * Verifies that an indexed element of a repeated string field can be read.
     *
     * <p>Given a message with three {@code list_values}, when {@code "list_values[1]"} is read by
     * name, then it returns the second element {@code "test2"}.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListValues("test1")
                .addListValues("test2")
                .addListValues("test3")
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        Object value = protoParsedMessage.getFieldByName("list_values[1]");
        Assert.assertEquals("test2", value.toString());
    }

    /**
     * Verifies that an indexed element of a repeated message field is returned as JSON.
     *
     * <p>Given a message with two {@code list_message_values}, when {@code "list_message_values[0]"}
     * is read by name, then it returns the first message as {@code {"order_number":"123"}}.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnValueAtSpecificIndexInRepeatedMessageField() throws IOException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("123"))
                .addListMessageValues(TestMessage.newBuilder().setOrderNumber("456"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        Object value = protoParsedMessage.getFieldByName("list_message_values[0]");
        Assert.assertEquals("{\"order_number\":\"123\"}", value.toString());
    }

    /**
     * Verifies that {@link ProtoParsedMessage#getFields()} includes enum fields with their default.
     *
     * <p>Given a default {@link TestTypesMessage}, when {@link ProtoParsedMessage#getFields()} is
     * called, then an {@code enum_value} field is present with the default value
     * {@code "CATEGORY_1"}.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldIncludeDefaultEnumFieldsOnGetFields() throws IOException {
        TestTypesMessage message = TestTypesMessage.getDefaultInstance();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        Map<SchemaField, Object> fields = protoParsedMessage.getFields();
        Optional<SchemaField> enumValue = fields.keySet().stream().filter(f -> f.getName().equals("enum_value")).findFirst();
        Assert.assertTrue(enumValue.isPresent());
        Assert.assertEquals("CATEGORY_1", fields.get(enumValue.get()));
    }

    /**
     * Returns the value of the named field from a parsed-fields map.
     *
     * @param fields the parsed fields keyed by {@link SchemaField}
     * @param name the field name to look up
     * @return the value mapped to the first field whose name matches {@code name}
     */
    private Object getFieldsValue(Map<SchemaField, Object> fields, String name) {
        return fields.entrySet().stream().filter(f -> f.getKey().getName().equals(name)).findFirst().get().getValue();
    }


    /**
     * Verifies that message-typed fields are exposed as nested {@link ProtoParsedMessage}s.
     *
     * <p>Given a {@link TestTypesMessage} with a singular and a repeated message field, when
     * {@link ProtoParsedMessage#getFields()} is called, then the singular {@code message_value} is a
     * {@link ProtoParsedMessage} and {@code list_message_values} is a list whose elements are
     * {@link ProtoParsedMessage}s.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldReturnParsedMessageIfFieldValueIsOfTypeMessage() throws IOException {
        TestMessage msg = TestMessage.newBuilder().setOrderNumber("order-number").build();
        TestTypesMessage message = TestTypesMessage.newBuilder().setMessageValue(msg).addAllListMessageValues(Arrays.asList(msg, msg)).build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        Map<SchemaField, Object> fields = protoParsedMessage.getFields();
        Assert.assertTrue(getFieldsValue(fields, "message_value") instanceof ProtoParsedMessage);
        List<?> listValue = (List<?>) getFieldsValue(fields, "list_message_values");
        Assert.assertEquals(2, listValue.size());
        Assert.assertTrue(listValue.get(0) instanceof ProtoParsedMessage);
    }

    /**
     * Returns whether a field with the given name is present in a parsed-fields map.
     *
     * @param fields the parsed fields keyed by {@link SchemaField}
     * @param name the field name to look for
     * @return {@code true} if any field's name matches {@code name}, {@code false} otherwise
     */
    private boolean isFieldPresent(Map<SchemaField, Object> fields, String name) {
        return fields.entrySet().stream().anyMatch(f -> f.getKey().getName().equals(name));
    }

    /**
     * Verifies that {@link ProtoParsedMessage#getFields()} includes proto default values for scalars.
     *
     * <p>Given a default {@link TestTypesMessage}, when {@link ProtoParsedMessage#getFields()} is
     * called, then numeric, enum, bytes and boolean fields carry their proto defaults while string,
     * message, repeated and timestamp fields are absent.</p>
     *
     * @throws IOException if parsing fails
     */
    @Test
    public void shouldIncludeDefaultValuesForMessage() throws IOException {
        TestTypesMessage message = TestTypesMessage.getDefaultInstance();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
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

    /**
     * Verifies that JSON conversion uses camelCase field names by default.
     *
     * <p>Given a {@link TestTypesMessage} with string, float and message values, when
     * {@link ProtoParsedMessage#toJson()} is called, then the resulting JSON uses camelCase keys such
     * as {@code stringValue}, {@code floatValue} and {@code messageValue}.</p>
     *
     * @throws InvalidProtocolBufferException if JSON conversion fails
     */
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
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        assertEquals(jsonObject.toString(), protoParsedMessage.toJson().toString());
    }

    /**
     * Verifies that converting a message with an out-of-range timestamp fails.
     *
     * <p>Given a {@link TestTypesMessage} whose {@code timestamp_value} is far out of range, when
     * {@link ProtoParsedMessage#toJson()} is called, then a {@link DeserializerException} is
     * thrown.</p>
     *
     * @throws InvalidProtocolBufferException if parsing the message fails
     */
    @Test
    public void shouldThrowExceptionForInvalidProtoMessage() throws InvalidProtocolBufferException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setTimestampValue(Timestamp.newBuilder().setSeconds(-99999999999999L).setNanos(0).build())
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        assertThrows(DeserializerException.class, protoParsedMessage::toJson);
    }

    /**
     * Verifies that converting a message with an unresolvable {@code Any} value fails.
     *
     * <p>Given a {@link TestTypesMessage} carrying an {@code Any} with an unknown type URL, when
     * {@link ProtoParsedMessage#toJson()} is called, then a {@link DeserializerException} is
     * thrown.</p>
     *
     * @throws InvalidProtocolBufferException if parsing the message fails
     */
    @Test
    public void shouldThrowExceptionForInvalidType() throws InvalidProtocolBufferException {
        TestTypesMessage message = TestTypesMessage
                .newBuilder()
                .setStringValue("test-string")
                .setAnyValue(Any.newBuilder().setTypeUrl("type-url").setValue(ByteString.copyFromUtf8("test-string")).build())
                .setMessageValue(TestMessage.newBuilder().setOrderNumber("order-1").setOrderDetails("order-details-1"))
                .build();
        Parser protoParser = StencilClientFactory.getClient().getParser(TestTypesMessage.class.getName());
        ProtoParsedMessage protoParsedMessage = new ProtoParsedMessage(protoParser.parse(message.toByteArray()), jsonPathConfig);
        assertThrows(DeserializerException.class, protoParsedMessage::toJson);
    }
}
