package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.*;
import com.gotocompany.depot.*;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.TestProtoUtil;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.jayway.jsonpath.Configuration;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;
import org.threeten.extra.Days;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Mockito.CALLS_REAL_METHODS;


/**
 * Unit tests for {@link BigQueryProtoStorageClient}, which converts protobuf {@link Message}s into a
 * {@link BigQueryPayload} of serialized rows for the BigQuery Storage Write API.
 *
 * <p>The fixture derives a protobuf descriptor from a {@link TableSchema} (via
 * {@link BQTableSchemaToProtoDescriptor}) modelled on {@code TestMessageBQ}, and wires a
 * {@link ProtoMessageParser} and a mocked {@link BigQueryProtoWriter} that returns that descriptor.
 * The tests convert messages and re-parse the resulting {@link ProtoRows} to assert the mapping of
 * primitive, case-insensitive, duration, map, nested, timestamp and struct fields, the placement of
 * (optionally namespaced) metadata, the routing of invalid records, and the partition-column
 * timestamp bounds enforced by BigQuery.</p>
 */
public class BigQueryProtoStorageClientTest {


    /** Protobuf descriptor derived from {@code testMessageBQSchema}; the conversion target. */
    private Descriptors.Descriptor testDescriptor;
    /** Client under test, converting messages into a {@link BigQueryPayload}. */
    private BigQueryProtoStorageClient converter;
    /** BigQuery table schema modelling {@code TestMessageBQ}, used to build the descriptor. */
    private TableSchema testMessageBQSchema;
    /** Parser used by the client to deserialize protobuf messages. */
    private ProtoMessageParser protoMessageParser;

    /**
     * Builds the descriptor, parser and client under test before each test.
     *
     * <p>Sets the proto, metadata and partition-key system properties, constructs the
     * {@code TestMessageBQ} {@link TableSchema}, converts it to a protobuf descriptor, and wires the
     * {@link BigQueryProtoStorageClient} around a mocked {@link BigQueryProtoWriter} that returns that
     * descriptor.</p>
     *
     * @throws IOException                               if parser or stencil setup fails
     * @throws Descriptors.DescriptorValidationException if the descriptor cannot be built from the
     *                                                   schema
     */
    @Before
    public void setUp() throws IOException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessageBQ");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        System.setProperty("SINK_BIGQUERY_TABLE_PARTITION_KEY", "created_at");
        System.setProperty("SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true");
        ClassLoadStencilClient stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(config))
                .build();
        protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        testMessageBQSchema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_offset")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_topic")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("load_time")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.TIMESTAMP)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_timestamp")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.TIMESTAMP)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("message_partition")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_number")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("created_at")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("aliases")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("discount")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("order_url")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("price")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DOUBLE)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("user_token")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.BYTES)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("counter")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("status")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("properties")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("trip_duration")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("seconds")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("nanos")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("current_state")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("key")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("value")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("updated_at")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("camelCase")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRING)
                        .build())
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(testMessageBQSchema);
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);
    }

    /**
     * Verifies conversion of primitive and repeated scalar fields.
     *
     * <p>Given a message with string, long, bytes, enum, counter and repeated alias fields, when
     * {@code convert} runs, then the single serialized row decodes back to those values, with the enum
     * rendered as its string name and the aliases preserved in order.</p>
     *
     * @throws Exception if the message cannot be parsed or converted
     */
    @Test
    public void shouldConvertPrimitiveFields() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setOrderNumber("order-no-112")
                .setOrderUrl("order-url-1")
                .setDiscount(1200L)
                .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                .setPrice(23)
                .setUserToken(ByteString.copyFrom("test-token".getBytes()))
                .setCounter(20)
                .setStatus(StatusBQ.COMPLETED)
                .addAliases("alias1").addAliases("alias2")
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        Assert.assertEquals(1, payload.getPayloadIndexes().size());
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals("order-no-112", convertedMessage.getField(testDescriptor.findFieldByName("order_number")));
        Assert.assertEquals("order-url-1", convertedMessage.getField(testDescriptor.findFieldByName("order_url")));
        Assert.assertEquals(1200L, convertedMessage.getField(testDescriptor.findFieldByName("discount")));
        Assert.assertEquals(ByteString.copyFrom("test-token".getBytes()), convertedMessage.getField(testDescriptor.findFieldByName("user_token")));
        List aliases = (List) convertedMessage.getField(testDescriptor.findFieldByName("aliases"));
        Assert.assertEquals("alias1", aliases.get(0));
        Assert.assertEquals("alias2", aliases.get(1));
        Assert.assertEquals(20L, convertedMessage.getField(testDescriptor.findFieldByName("counter")));
        Assert.assertEquals("COMPLETED", convertedMessage.getField(testDescriptor.findFieldByName("status")));
    }

    /**
     * Verifies that field lookup tolerates case differences between proto and BigQuery names.
     *
     * <p>Given a message setting {@code camelCase}, when {@code convert} runs, then the serialized row
     * exposes the value under the lower-cased {@code camelcase} field of the target descriptor.</p>
     *
     * @throws InvalidProtocolBufferException if the serialized row cannot be parsed
     */
    @Test
    public void shouldReturnCaseInsensitiveFields() throws InvalidProtocolBufferException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCamelCase("testing")
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals("testing", convertedMessage.getField(testDescriptor.findFieldByName("camelcase")));
    }

    /**
     * Verifies that a duration field is converted to a nested seconds/nanos record.
     *
     * <p>Given a message with a {@code trip_duration} of 1234 seconds and 231 nanos, when
     * {@code convert} runs, then the serialized row's {@code trip_duration} record exposes those
     * seconds and nanos values.</p>
     *
     * @throws IOException if the serialized row cannot be parsed
     */
    @Test
    public void shouldReturnDurationField() throws IOException {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                .setTripDuration(Duration.newBuilder().setSeconds(1234L).setNanos(231).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage tripDuration = ((DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("trip_duration")));
        Assert.assertEquals(1234L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("seconds")));
        Assert.assertEquals(231L, tripDuration.getField(tripDuration.getDescriptorForType().findFieldByName("nanos")));
    }

    /**
     * Verifies that a protobuf map is converted to a repeated key/value record.
     *
     * <p>Given a message with four {@code current_state} map entries, when {@code convert} runs, then
     * the serialized row's {@code current_state} repeated record contains the four key/value pairs.</p>
     *
     * @throws Exception if the serialized row cannot be parsed
     */
    @Test
    public void shouldReturnMapField() throws Exception {
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .putCurrentState("k4", "v4")
                .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                .putCurrentState("k3", "v3")
                .putCurrentState("k1", "v1")
                .putCurrentState("k2", "v2")
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        List<Object> currentState = ((List<Object>) convertedMessage.getField(testDescriptor.findFieldByName("current_state")));
        List<Tuple<String, String>> actual = currentState.stream().map(o -> {
            Map<String, String> values = ((DynamicMessage) o).getAllFields().entrySet().stream().collect(
                    Collectors.toMap(s -> s.getKey().getName(), s -> s.getValue().toString())
            );
            return new Tuple<>(values.get("key"), values.get("value"));
        }).collect(Collectors.toList());
        actual.sort(Comparator.comparing(Tuple::getFirst));
        List<Tuple<String, String>> expected = new ArrayList<Tuple<String, String>>() {{
            add(new Tuple<>("k1", "v1"));
            add(new Tuple<>("k2", "v2"));
            add(new Tuple<>("k3", "v3"));
            add(new Tuple<>("k4", "v4"));
        }};
        Assert.assertEquals(expected, actual);
    }

    /**
     * Verifies conversion of nested and repeated message fields plus repeated scalars.
     *
     * <p>Given a {@code TestNestedRepeatedMessageBQ} with a single nested message, two repeated nested
     * messages, a scalar number and three repeated numbers, when {@code convert} runs against the
     * matching descriptor, then the serialized row exposes the nested order numbers, the scalar number
     * and the three repeated numbers in order.</p>
     *
     * @throws Descriptors.DescriptorValidationException if the nested descriptor cannot be built
     * @throws IOException                               if the serialized row cannot be parsed
     */
    @Test
    public void shouldReturnComplexAndNestedType() throws Descriptors.DescriptorValidationException, IOException {
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("single_message")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_message")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addAllFields(testMessageBQSchema.getFieldsList())
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("number_field")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("repeated_number_field")
                        .setMode(TableFieldSchema.Mode.REPEATED)
                        .setType(TableFieldSchema.Type.INT64)
                        .build())
                .build();
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestNestedRepeatedMessageBQ");
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);

        Instant now = Instant.now();
        TestMessageBQ singleMessage = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested1 = TestProtoUtil.generateTestMessage(now);
        TestMessageBQ nested2 = TestProtoUtil.generateTestMessage(now);
        TestNestedRepeatedMessageBQ message = TestNestedRepeatedMessageBQ.newBuilder()
                .setNumberField(123)
                .setSingleMessage(singleMessage)
                .addRepeatedMessage(nested1)
                .addRepeatedMessage(nested2)
                .addRepeatedNumberField(11)
                .addRepeatedNumberField(12)
                .addRepeatedNumberField(13)
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, message.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);

        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage sm1 = (DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("single_message"));
        Assert.assertEquals(singleMessage.getOrderNumber(), sm1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        List<DynamicMessage> nestedMessage = (List) convertedMessage.getField(testDescriptor.findFieldByName("repeated_message"));
        Assert.assertEquals(2, nestedMessage.size());
        DynamicMessage nestedMessage1 = nestedMessage.get(0);
        DynamicMessage nestedMessage2 = nestedMessage.get(1);
        Assert.assertEquals(nested1.getOrderNumber(), nestedMessage1.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals(nested2.getOrderNumber(), nestedMessage2.getField(sm1.getDescriptorForType().findFieldByName("order_number")));
        Assert.assertEquals(123L, convertedMessage.getField(testDescriptor.findFieldByName("number_field")));
        List<Long> repeatedNumbers = (List) convertedMessage.getField(testDescriptor.findFieldByName("repeated_number_field"));
        Assert.assertEquals(3, repeatedNumbers.size());
        Assert.assertEquals(Long.valueOf(11), repeatedNumbers.get(0));
        Assert.assertEquals(Long.valueOf(12), repeatedNumbers.get(1));
        Assert.assertEquals(Long.valueOf(13), repeatedNumbers.get(2));
    }

    /**
     * Verifies that timestamp fields are converted to microsecond values.
     *
     * <p>Given a message with a {@code created_at} and two {@code updated_at} timestamps set to
     * {@code now}, when {@code convert} runs, then the serialized row exposes those timestamps as
     * microseconds since the epoch.</p>
     *
     * @throws IOException if the serialized row cannot be parsed
     */
    @Test
    public void shouldConvertTimeStamp() throws IOException {
        Instant now = Instant.now();
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        long createdAt = (long) convertedMessage.getField(testDescriptor.findFieldByName("created_at"));
        // Microseconds
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(now.getEpochSecond()), createdAt);
        List<Object> updatedAt = (List) convertedMessage.getField(testDescriptor.findFieldByName("updated_at"));
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(now.getEpochSecond()), updatedAt.get(0));
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(now.getEpochSecond()), updatedAt.get(1));
    }

    /**
     * Verifies that a struct field is serialized to a JSON string.
     *
     * <p>Given a {@code properties} struct holding a string, a list, a boolean and a number, when
     * {@code convert} runs, then the serialized row's {@code properties} column equals the equivalent
     * JSON document.</p>
     *
     * @throws IOException if the serialized row cannot be parsed
     */
    @Test
    public void shouldConvertStruct() throws IOException {
        ListValue.Builder builder = ListValue.newBuilder();
        ListValue listValue = builder
                .addValues(Value.newBuilder().setNumberValue(1).build())
                .addValues(Value.newBuilder().setNumberValue(2).build())
                .addValues(Value.newBuilder().setNumberValue(3).build())
                .build();
        Struct value = Struct.newBuilder()
                .putFields("string", Value.newBuilder().setStringValue("string_val").build())
                .putFields("list", Value.newBuilder().setListValue(listValue).build())
                .putFields("boolean", Value.newBuilder().setBoolValue(true).build())
                .putFields("number", Value.newBuilder().setNumberValue(123.45).build())
                .build();

        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setCreatedAt(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                .setProperties(value)
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        String properties = (String) (convertedMessage.getField(testDescriptor.findFieldByName("properties")));
        String expected = "{\n"
                + "  \"number\": 123.45,\n"
                + "  \"string\": \"string_val\",\n"
                + "  \"list\": [\n"
                + "    1,\n"
                + "    2,\n"
                + "    3\n"
                + "  ],\n"
                + "  \"boolean\": true\n"
                + "}\n";
        JSONAssert.assertEquals(expected, properties, true);
    }

    /**
     * Verifies that message metadata is written as top-level columns when no namespace is set.
     *
     * <p>Given two messages carrying partition, topic, offset, load-time and timestamp metadata, when
     * {@code convert} runs, then both serialized rows expose those metadata values (timestamps in
     * microseconds) at the top level.</p>
     *
     * @throws InvalidProtocolBufferException if a serialized row cannot be parsed
     */
    @Test
    public void shouldHaveMetadataOnPayload() throws InvalidProtocolBufferException {
        Instant now = Instant.now();
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 143),
                    new Tuple<>("load_time", now.toEpochMilli()),
                    new Tuple<>("message_timestamp", now.toEpochMilli()))
            );
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 144L),
                    new Tuple<>("load_time", now.toEpochMilli()),
                    new Tuple<>("message_timestamp", now.toEpochMilli()))
            );
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(2, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals(10L, convertedMessage.getField(testDescriptor.findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", convertedMessage.getField(testDescriptor.findFieldByName("message_topic")));
        Assert.assertEquals(143L, convertedMessage.getField(testDescriptor.findFieldByName("message_offset")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), convertedMessage.getField(testDescriptor.findFieldByName("load_time")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), convertedMessage.getField(testDescriptor.findFieldByName("message_timestamp")));

        serializedRows = protoPayload.getSerializedRows(1);
        convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        Assert.assertEquals(10L, convertedMessage.getField(testDescriptor.findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", convertedMessage.getField(testDescriptor.findFieldByName("message_topic")));
        Assert.assertEquals(144L, convertedMessage.getField(testDescriptor.findFieldByName("message_offset")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), convertedMessage.getField(testDescriptor.findFieldByName("load_time")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), convertedMessage.getField(testDescriptor.findFieldByName("message_timestamp")));
    }


    /**
     * Verifies that message metadata is nested under a namespace record when one is configured.
     *
     * <p>Given a {@code __kafka_metadata} namespace and a matching nested descriptor, when
     * {@code convert} runs, then the serialized row exposes the metadata under the
     * {@code __kafka_metadata} record (timestamps in microseconds).</p>
     *
     * @throws InvalidProtocolBufferException            if the serialized row cannot be parsed
     * @throws Descriptors.DescriptorValidationException if the namespaced descriptor cannot be built
     */
    @Test
    public void shouldHaveMetadataOnPayloadWithNameSpace() throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "__kafka_metadata");
        TableSchema schema = TableSchema.newBuilder()
                .addFields(TableFieldSchema.newBuilder()
                        .setName("created_at")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.DATETIME)
                        .build())
                .addFields(TableFieldSchema.newBuilder()
                        .setName("__kafka_metadata")
                        .setMode(TableFieldSchema.Mode.NULLABLE)
                        .setType(TableFieldSchema.Type.STRUCT)
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_offset")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_topic")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.STRING)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("load_time")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.TIMESTAMP)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_timestamp")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.TIMESTAMP)
                                .build())
                        .addFields(TableFieldSchema.newBuilder()
                                .setName("message_partition")
                                .setMode(TableFieldSchema.Mode.NULLABLE)
                                .setType(TableFieldSchema.Type.INT64)
                                .build())
                        .build())
                .build();
        testDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(schema);
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        BigQueryProtoWriter writer = Mockito.mock(BigQueryProtoWriter.class);
        converter = new BigQueryProtoStorageClient(writer, config, protoMessageParser);
        Mockito.when(writer.getDescriptor()).thenReturn(testDescriptor);

        Instant now = Instant.now();
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();

        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(
                    null,
                    m1.toByteArray(),
                    new Tuple<>("message_partition", 10),
                    new Tuple<>("message_topic", "test-topic"),
                    new Tuple<>("message_offset", 143),
                    new Tuple<>("load_time", now.toEpochMilli()),
                    new Tuple<>("message_timestamp", now.toEpochMilli()))
            );
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        DynamicMessage metadata = (DynamicMessage) convertedMessage.getField(testDescriptor.findFieldByName("__kafka_metadata"));
        Assert.assertEquals(10L, metadata.getField(metadata.getDescriptorForType().findFieldByName("message_partition")));
        Assert.assertEquals("test-topic", metadata.getField(metadata.getDescriptorForType().findFieldByName("message_topic")));
        Assert.assertEquals(143L, metadata.getField(metadata.getDescriptorForType().findFieldByName("message_offset")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), metadata.getField(metadata.getDescriptorForType().findFieldByName("load_time")));
        Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(now.toEpochMilli()), metadata.getField(metadata.getDescriptorForType().findFieldByName("message_timestamp")));
    }

    /**
     * Verifies that unparseable messages are recorded as invalid while valid ones are kept.
     *
     * <p>Given three messages where the middle one carries invalid bytes, when {@code convert} runs,
     * then two rows are serialized and the payload maps valid positions back to input indexes
     * {@code 0} and {@code 2}; the invalid record carries a {@link ErrorType#DESERIALIZATION_ERROR}
     * with the truncated-message error text while the valid record has no error.</p>
     *
     * @throws InvalidProtocolBufferException if a serialized row cannot be parsed
     */
    @Test
    public void shouldReturnInvalidRecords() throws InvalidProtocolBufferException {
        Instant now = Instant.now();
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray(), new Tuple<>("message_offset", 11)));
            add(new Message(null, "invalid".getBytes(StandardCharsets.UTF_8), new Tuple<>("message_offset", 12)));
            add(new Message(null, m1.toByteArray(), new Tuple<>("message_offset", 13)));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(2, protoPayload.getSerializedRowsCount());
        Assert.assertEquals(2, payload.getPayloadIndexes().size());
        Assert.assertTrue(payload.getPayloadIndexes().contains(0L));
        Assert.assertTrue(payload.getPayloadIndexes().contains(1L));
        Assert.assertFalse(payload.getPayloadIndexes().contains(2L));
        Assert.assertEquals(0L, payload.getInputIndex(0L));
        Assert.assertEquals(2L, payload.getInputIndex(1L));
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        long createdAt = (long) convertedMessage.getField(testDescriptor.findFieldByName("created_at"));
        // Microseconds
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(now.getEpochSecond()), createdAt);

        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        BigQueryRecordMeta validRecord = metas.get(0);
        BigQueryRecordMeta invalidRecord = metas.get(1);
        Assert.assertTrue(validRecord.isValid());
        Assert.assertFalse(invalidRecord.isValid());
        Assert.assertNull(validRecord.getErrorInfo());
        Assert.assertNotNull(invalidRecord.getErrorInfo());
        Assert.assertEquals(0, validRecord.getInputIndex());
        Assert.assertEquals(1, invalidRecord.getInputIndex());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, invalidRecord.getErrorInfo().getErrorType());
        Assert.assertEquals("While parsing a protocol message, the input ended unexpectedly in the middle of a field.  This could mean either that the input has been truncated or that an embedded message misreported its own length.",
                invalidRecord.getErrorInfo().getException().getMessage());
    }

    /**
     * Verifies that a partition timestamp older than five years is rejected.
     *
     * <p>Given a {@code created_at} partition value 1826 days in the past, when {@code convert} runs,
     * then no rows are serialized and the single record is an {@link ErrorType#INVALID_MESSAGE_ERROR}
     * whose message reports the value is outside the allowed date range.</p>
     *
     * @throws IOException if conversion fails
     */
    @Test
    public void shouldNotConvertFiveYearsOldTimeStamp() throws IOException {
        Instant moreThanFiveYears = Instant.now().minus(Days.of(1826));
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(moreThanFiveYears.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanFiveYears.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanFiveYears.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(0, protoPayload.getSerializedRowsCount());
        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        Assert.assertEquals(1, metas.size());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, metas.get(0).getErrorInfo().getErrorType());
        Assert.assertTrue(metas.get(0).getErrorInfo().getException().getMessage()
                .contains("is outside the allowed bounds. You can only stream to date range within 1825 days in the past and 366 days in the future relative to the current date."));
    }

    /**
     * Verifies that out-of-range timestamps are tolerated on non-partition columns.
     *
     * <p>Given an in-range {@code created_at} partition value but far out-of-range {@code updated_at}
     * values, when {@code convert} runs, then the row is serialized successfully and the record carries
     * no error.</p>
     *
     * @throws IOException if conversion fails
     */
    @Test
    public void shouldConvertAnyTimeStampIfNotPartitionColumn() throws IOException {
        Instant moreThanFiveYears = Instant.now().minus(Days.of(18216));
        Instant lessThanFiveYears = Instant.now().minus(Days.of(100));
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(lessThanFiveYears.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanFiveYears.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanFiveYears.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        Assert.assertEquals(1, metas.size());
        Assert.assertNull(metas.get(0).getErrorInfo());
    }

    /**
     * Verifies that a partition timestamp more than one year in the future is rejected.
     *
     * <p>Given a {@code created_at} partition value far in the future, when {@code convert} runs, then
     * no rows are serialized and the single record is an {@link ErrorType#INVALID_MESSAGE_ERROR} whose
     * message reports the value is outside the allowed date range.</p>
     *
     * @throws IOException if conversion fails
     */
    @Test
    public void shouldNotConvertMoreThanOneYearFutureTimeStamp() throws IOException {
        Instant moreThanOneYear = Instant.now().plus(Days.of(10000));
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(moreThanOneYear.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanOneYear.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(moreThanOneYear.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(0, protoPayload.getSerializedRowsCount());
        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        Assert.assertEquals(1, metas.size());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, metas.get(0).getErrorInfo().getErrorType());
        Assert.assertTrue(metas.get(0).getErrorInfo().getException().getMessage()
                .contains("is outside the allowed bounds. You can only stream to date range within 1825 days in the past and 366 days in the future relative to the current date."));
    }

    /**
     * Verifies that an unrepresentable timestamp value is rejected.
     *
     * <p>Given an {@code updated_at} timestamp built from an absurd epoch second, when {@code convert}
     * runs, then no rows are serialized and the single record is an
     * {@link ErrorType#INVALID_MESSAGE_ERROR} reporting the value is outside the allowed bounds in
     * BigQuery.</p>
     *
     * @throws IOException if conversion fails
     */
    @Test
    public void shouldNotConvertIfInvalidTimeStamp() throws IOException {
        Instant now = Instant.now();
        Instant invalid = Instant.ofEpochSecond(1111111111111111L);
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(invalid.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(0, protoPayload.getSerializedRowsCount());
        List<BigQueryRecordMeta> metas = new ArrayList<>();
        for (BigQueryRecordMeta r : payload) {
            metas.add(r);
        }
        Assert.assertEquals(1, metas.size());
        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, metas.get(0).getErrorInfo().getErrorType());
        Assert.assertTrue(metas.get(0).getErrorInfo().getException().getMessage()
                .contains("is outside the allowed bounds in BQ"));
    }

    /**
     * Verifies that timestamps just within the allowed bounds are converted.
     *
     * <p>Given a {@code created_at} partition value 1824 days in the past and {@code updated_at} values
     * 365 days in the future and 1824 days in the past, when {@code convert} runs, then the row is
     * serialized and those timestamps are exposed as microseconds.</p>
     *
     * @throws IOException if the serialized row cannot be parsed
     */
    @Test
    public void shouldConvertTimeStampCloseToLimits() throws IOException {
        Instant past = Instant.now().minus(Days.of(1824));
        Instant future = Instant.now().plus(Days.of(365));
        TestMessageBQ m1 = TestMessageBQ.newBuilder()
                .setCreatedAt(Timestamp.newBuilder().setSeconds(past.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(future.getEpochSecond()).build())
                .addUpdatedAt(Timestamp.newBuilder().setSeconds(past.getEpochSecond()).build())
                .build();
        List<Message> inputList = new ArrayList<Message>() {{
            add(new Message(null, m1.toByteArray()));
        }};
        BigQueryPayload payload = converter.convert(inputList);
        ProtoRows protoPayload = (ProtoRows) payload.getPayload();
        Assert.assertEquals(1, protoPayload.getSerializedRowsCount());
        ByteString serializedRows = protoPayload.getSerializedRows(0);
        DynamicMessage convertedMessage = DynamicMessage.parseFrom(testDescriptor, serializedRows);
        long createdAt = (long) convertedMessage.getField(testDescriptor.findFieldByName("created_at"));
        // Microseconds
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(past.getEpochSecond()), createdAt);
        List<Object> updatedAt = (List) convertedMessage.getField(testDescriptor.findFieldByName("updated_at"));
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(future.getEpochSecond()), updatedAt.get(0));
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(past.getEpochSecond()), updatedAt.get(1));
    }
}
