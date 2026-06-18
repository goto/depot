package com.gotocompany.depot.bigquery.converter;

import com.google.api.client.util.DateTime;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Value;
import com.google.protobuf.util.Timestamps;
import com.gotocompany.depot.StatusBQ;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.TestMessageBQ;
import com.gotocompany.depot.TestTypesMessage;
import com.gotocompany.depot.bigquery.TestMessageBuilder;
import com.gotocompany.depot.bigquery.TestMetadata;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import com.jayway.jsonpath.Configuration;
import groovy.lang.Tuple3;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MessageRecordConverter} when parsing Protobuf-encoded messages into BigQuery
 * {@link Records}.
 *
 * <p>The fixture configures a {@link ProtoMessageParser} backed by a {@link ClassLoadStencilClient}
 * (or, in some tests, a mocked {@link StencilClient}) and a {@link BigQuerySinkConfig} derived from
 * system properties. The tests assert that valid protobuf messages convert to columns plus metadata,
 * that null-valued, malformed and unknown-field messages are routed to invalid records with the
 * correct {@link ErrorType}, that metadata namespacing is honoured, and that individual field types
 * (timestamp, struct, enum, bytes and {@code NaN} floating point) are converted or rejected as
 * expected.</p>
 */
public class MessageRecordConverterTest {
    /** Converter under test, rebuilt in {@link #setUp()} and, in some tests, with bespoke config. */
    private MessageRecordConverter recordConverter;
    /** Mocked stencil client (with real methods) used to resolve protobuf descriptors. */
    @Mock
    private ClassLoadStencilClient stencilClient;
    /** Reference instant captured during setup and reused to build metadata and timestamps. */
    private Instant now;

    /**
     * Configures the converter under test from system properties before each test.
     *
     * <p>Sets the proto message class and metadata column properties, builds a
     * {@link ProtoMessageParser} over a real-methods {@link ClassLoadStencilClient} and a
     * {@link BigQuerySinkConfig}, and captures the reference {@code now} instant.</p>
     *
     * @throws IOException if the stencil client or parser cannot be initialized
     */
    @Before
    public void setUp() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,message_topic=string,load_time=timestamp,message_timestamp=timestamp,message_partition=integer");
        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);

        recordConverter = new MessageRecordConverter(protoMessageParser, bigQuerySinkConfig);

        now = Instant.now();
    }

    /**
     * Verifies that valid protobuf messages convert to columns plus metadata.
     *
     * <p>Given two consumer records, when {@code convert} runs, then both become valid records whose
     * columns contain the order fields and the expected metadata columns for their offsets.</p>
     */
    @Test
    public void shouldGetRecordForBQFromConsumerRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));


        Map<String, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("order_number", "order-2");
        record2ExpectedColumns.put("order_url", "order-url-2");
        record2ExpectedColumns.put("order_details", "order-details-2");
        record2ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record2Offset, now));
        List<Message> messages = Arrays.asList(record1, record2);

        Records records = recordConverter.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        Map<String, Object> record2Columns = records.getValidRecords().get(1).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record2ExpectedColumns.size(), record2Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(record2ExpectedColumns, record2Columns);
    }

    /**
     * Verifies that a record with an empty (null) value is dropped from the valid output.
     *
     * <p>Given one normal record and one record built with a {@code null} value, when {@code convert}
     * runs, then only the normal record is returned as valid, with its expected columns.</p>
     */
    @Test
    public void shouldIgnoreNullRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    /**
     * Verifies that a null-valued record is excluded from the valid records.
     *
     * <p>Given one normal record and one record with a {@code null} value, when {@code convert} runs,
     * then exactly one valid record (the normal one) is produced with its expected columns.</p>
     */
    @Test
    public void shouldReturnInvalidRecordsWhenGivenNullRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = TestMessageBuilder.withMetadata(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    /**
     * Verifies that metadata columns are not nested under a namespace when none is configured.
     *
     * <p>Given an empty metadata namespace, when a record is converted, then its metadata columns are
     * placed at the top level of the column map and the configured namespace is the empty string.</p>
     */
    @Test
    public void shouldNotNamespaceMetadataFieldWhenNamespaceIsNotProvided() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(sinkConfig.getBqMetadataNamespace(), "");
    }

    /**
     * Verifies that metadata columns are nested under the configured namespace.
     *
     * <p>Given a {@code metadata_ns} namespace, when a record is converted, then the metadata columns
     * are grouped under a single {@code metadata_ns} column rather than placed at the top level.</p>
     */
    @Test
    public void shouldNamespaceMetadataFieldWhenNamespaceIsProvided() {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_ns");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(protoMessageParser, sinkConfig);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.put(sinkConfig.getBqMetadataNamespace(), TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }


    /**
     * Verifies that a message with an unparseable protobuf value is reported as invalid.
     *
     * <p>Given one valid record and one record carrying invalid value bytes, when {@code convert}
     * runs, then exactly one invalid record is produced.</p>
     */
    @Test
    public void shouldReturnInvalidRecordsGivenInvalidProtobufMessage() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("topic", record2Offset.getTopic()),
                new Tuple<>("partition", record2Offset.getPartition()));
        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);
        assertEquals(1, records.getInvalidRecords().size());
    }

    /**
     * Verifies that an invalid record preserves its metadata and is classified as a deserialization
     * error.
     *
     * <p>Given one valid record and one record with invalid value bytes but full metadata, when
     * {@code convert} runs, then there is one valid and one invalid record; the invalid record has
     * empty columns, retains the original metadata and carries
     * {@link ErrorType#DESERIALIZATION_ERROR}.</p>
     */
    @Test
    public void shouldWriteToErrorWriterInvalidRecords() {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestMetadata record2Offset = new TestMetadata("topic1", 2, 102, Instant.now().toEpochMilli(), now.toEpochMilli());
        Message record1 = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                new Tuple<>("message_topic", record2Offset.getTopic()),
                new Tuple<>("message_partition", record2Offset.getPartition()),
                new Tuple<>("message_offset", record2Offset.getOffset()),
                new Tuple<>("message_timestamp", new DateTime(record2Offset.getTimestamp())),
                new Tuple<>("load_time", new DateTime(record2Offset.getLoadTime())));

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("order_number", "order-1");
        record1ExpectedColumns.put("order_url", "order-url-1");
        record1ExpectedColumns.put("order_details", "order-details-1");
        record1ExpectedColumns.putAll(TestMessageBuilder.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(new HashMap<>(), records.getInvalidRecords().get(0).getColumns());
        assertEquals(record2.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    /**
     * Verifies that messages containing unknown protobuf fields are rejected when disallowed.
     *
     * <p>Given {@code allow-unknown-fields} disabled and a parsed message carrying an unknown field,
     * when {@code convert} runs, then no valid records are produced and the single invalid record is
     * classified as {@link ErrorType#UNKNOWN_FIELDS_ERROR} while retaining the original metadata.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldReturnInvalidRecordsWhenUnknownFieldsFound() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false");
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        MessageParser mockParser = mock(MessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message consumerRecord = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ParsedMessage parsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);

        when(mockParser.parse(consumerRecord, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage")).thenReturn(parsedMessage);

        recordConverter = new MessageRecordConverter(mockParser, bigQuerySinkConfig);

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        assertEquals(0, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
        assertEquals(consumerRecord.getMetadata(), records.getInvalidRecords().get(0).getMetadata());
    }

    /**
     * Verifies that unknown protobuf fields are tolerated when explicitly allowed.
     *
     * <p>Given {@code allow-unknown-fields} enabled and a parsed message carrying an unknown field,
     * when {@code convert} runs, then the message becomes a single valid record (with no invalid
     * records) whose metadata columns are derived from the configured metadata column types.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldIgnoreUnknownFieldsIfTheConfigIsSet() throws IOException {
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        MessageParser mockParser = mock(MessageParser.class);

        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), Instant.now().toEpochMilli());
        Message consumerRecord = TestMessageBuilder.withMetadata(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessage.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(10, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ParsedMessage parsedMessage = new ProtoParsedMessage(dynamicMessage, jsonPathConfig);
        when(mockParser.parse(consumerRecord, SinkConnectorSchemaMessageMode.LOG_MESSAGE, "com.gotocompany.depot.TestMessage")).thenReturn(parsedMessage);

        recordConverter = new MessageRecordConverter(mockParser, bigQuerySinkConfig
        );

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages);

        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Map<String, Object> metadata = consumerRecord.getMetadata();
        Map<String, Object> finalMetadata = metadataColumnsTypes.stream().collect(Collectors.toMap(TupleString::getFirst, t -> {
            String key = t.getFirst();
            String dataType = t.getSecond();
            Object value = metadata.get(key);
            if (value instanceof Long && dataType.equals("timestamp")) {
                value = new DateTime((long) value);
            }
            return value;
        }));
        Record record = new Record(consumerRecord.getMetadata(), finalMetadata, 0, null);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        assertEquals(record, records.getValidRecords().get(0));
    }

    /**
     * Builds a converter, input messages and expected metadata for a single-field type test.
     *
     * <p>Creates a {@link TestMessageBQ} with the given field set to {@code value}, stubs a
     * {@link StencilClient} to parse it into a {@link DynamicMessage}, and wires a
     * {@link ProtoMessageParser} and {@link MessageRecordConverter} around it.</p>
     *
     * @param fieldName the {@link TestMessageBQ} field to populate
     * @param value     the value to set on that field
     * @return a tuple of the converter, the single-message input list and the expected metadata
     *         columns
     * @throws InvalidProtocolBufferException if the constructed message cannot be parsed
     */
    private Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> setupForTypeTest(String fieldName, Object value) throws InvalidProtocolBufferException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        Descriptors.FieldDescriptor fd = TestMessageBQ.getDescriptor().findFieldByName(fieldName);
        TestMessageBQ message = TestMessageBQ.newBuilder()
                .setField(fd, value)
                .build();
        DynamicMessage d = DynamicMessage.parseFrom(TestMessageBQ.getDescriptor(), message.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(d);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                bigQuerySinkConfig);
        Map<String, Object> metadataColumns = TestMessageBuilder.metadataColumns(record1Offset, now);
        return new Tuple3<>(messageRecordConverter, messages, metadataColumns);
    }

    /**
     * Verifies that a protobuf timestamp field is converted to a {@link DateTime} column.
     *
     * <p>Given a {@code created_at} timestamp set to {@code now}, when the message is converted, then
     * the single valid record's {@code created_at} column equals the corresponding
     * {@link DateTime}.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldConvertTimestampFieldToDateTime() throws IOException {
        Timestamp timestampData = Timestamps.fromMillis(now.toEpochMilli());
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("created_at", timestampData);
        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();
        DateTime expectedDayTime = new DateTime(now.toEpochMilli());

        Records records = converter.convert(inputData);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(expectedDayTime, record1Columns.get("created_at"));
    }

    /**
     * Verifies that a protobuf struct field is serialized to a JSON string column.
     *
     * <p>Given a {@code properties} struct with string, number, boolean and null entries, when the
     * message is converted, then the single valid record's {@code properties} column is the equivalent
     * JSON object.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldConvertStructFieldToMap() throws IOException {
        Struct structData = Struct.newBuilder()
                .putFields("name", Value.newBuilder().setStringValue("goto").build())
                .putFields("age", Value.newBuilder().setNumberValue(Double.parseDouble("10")).build())
                .putFields("null_key", Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build())
                .putFields("bool", Value.newBuilder().setBoolValue(true).build())
                .build();
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("properties", structData);
        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        String expectedProperties = "{\"name\":\"goto\",\"age\":10,\"bool\": true, \"null_key\": null}";

        Records records = converter.convert(inputData);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();

        assertEquals(new JSONObject(expectedProperties).toString(), new JSONObject((String) record1Columns.get("properties")).toString());
    }

    /**
     * Verifies that {@code NaN} float and double values are rejected as a deserialization error.
     *
     * <p>Given a {@link TestTypesMessage} with {@code NaN} float and double values, when the message
     * is converted, then the single invalid record carries an {@link IllegalArgumentException}
     * classified as {@link ErrorType#DESERIALIZATION_ERROR}.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldThrowExceptionWhenFloatingPointIsNaN() throws IOException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder().setFloatValue(Float.NaN).setDoubleValue(Double.NaN).setStringValue("test").build();
        DynamicMessage message = DynamicMessage.parseFrom(TestTypesMessage.getDescriptor(), testTypesMessage.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(message);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));
        Records records = messageRecordConverter.convert(messages);
        assertEquals(IllegalArgumentException.class, records.getInvalidRecords().get(0).getErrorInfo().getException().getClass());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    /**
     * Verifies that a {@code NaN} double value is rejected as a deserialization error.
     *
     * <p>Given a {@link TestTypesMessage} with a {@code NaN} double value, when the message is
     * converted, then the single invalid record carries an {@link IllegalArgumentException} classified
     * as {@link ErrorType#DESERIALIZATION_ERROR}.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldThrowExceptionWhenDoubleIsNaN() throws IOException {
        TestMetadata record1Offset = new TestMetadata("topic1", 1, 101, Instant.now().toEpochMilli(), now.toEpochMilli());
        TestTypesMessage testTypesMessage = TestTypesMessage.newBuilder().setDoubleValue(Double.NaN).setStringValue("test").build();
        DynamicMessage message = DynamicMessage.parseFrom(TestTypesMessage.getDescriptor(), testTypesMessage.toByteArray());
        Message consumerRecord = new Message(
                message.toByteArray(),
                message.toByteArray(),
                new Tuple<>("message_topic", record1Offset.getTopic()),
                new Tuple<>("message_partition", record1Offset.getPartition()),
                new Tuple<>("message_offset", record1Offset.getOffset()),
                new Tuple<>("message_timestamp", record1Offset.getTimestamp()),
                new Tuple<>("load_time", record1Offset.getLoadTime()));
        List<Message> messages = Collections.singletonList(consumerRecord);
        StencilClient client1 = Mockito.mock(StencilClient.class);
        when(client1.parse(anyString(), any())).thenReturn(message);
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(bigQuerySinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(client1, jsonPathConfig);
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(protoMessageParser,
                bigQuerySinkConfig);
        Records records = messageRecordConverter.convert(messages);
        assertEquals(IllegalArgumentException.class, records.getInvalidRecords().get(0).getErrorInfo().getException().getClass());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, records.getInvalidRecords().get(0).getErrorInfo().getErrorType());
    }

    /**
     * Verifies that a protobuf enum field is converted to its string name.
     *
     * <p>Given a {@code status} field set to {@code CANCELLED}, when the message is converted, then the
     * single valid record's {@code status} column is the string {@code "CANCELLED"}.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldConvertEnumToString() throws IOException {

        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("status", StatusBQ.CANCELLED.getValueDescriptor());

        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        Records records = converter.convert(inputData);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();

        assertEquals("CANCELLED", record1Columns.get("status"));
    }

    /**
     * Verifies that a protobuf bytes field is converted to its base64 string representation.
     *
     * <p>Given a {@code user_token} bytes field, when the message is converted, then the single valid
     * record's {@code user_token} column equals the base64 encoding of the original bytes.</p>
     *
     * @throws IOException if the message cannot be parsed
     */
    @Test
    public void shouldConvertBytesToString() throws IOException {
        byte[] byteData = "byteDataTest".getBytes(StandardCharsets.UTF_8);
        Tuple3<MessageRecordConverter, List<Message>, Map<String, Object>> testData = setupForTypeTest("user_token", ByteString.copyFrom(byteData));

        MessageRecordConverter converter = testData.getV1();
        List<Message> inputData = testData.getV2();

        Records records = converter.convert(inputData);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        String expected = BaseEncoding.base64().encode(byteData);

        assertEquals(expected, record1Columns.get("user_token"));
    }
}
