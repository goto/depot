package com.gotocompany.depot.bigtable.parser;

import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.proto.ProtoJsonProvider;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.utils.MessageConfigUtils;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.jayway.jsonpath.Configuration;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link BigTableRecordParser}, which turns a batch of {@link Message}s into
 * {@link BigTableRecord}s using a {@link ProtoMessageParser}, a {@link BigTableRowKeyParser} and a
 * {@link BigTableSchema}.
 *
 * <p>The {@link #setUp()} fixture seeds the proto message class, message mode, column-family mapping
 * and row-key template as system properties, then wires a real parser pipeline around a
 * {@link ClassLoadStencilClient} stubbed with {@code CALLS_REAL_METHODS} and two booking-log
 * {@link Message}s. The happy-path tests reconfigure the column-family mapping to cover plain,
 * complex (message), nested and nested-timestamp fields and assert that the produced records are
 * valid. The error-path tests replace the collaborators with Mockito mocks
 * ({@link #mockMessageParser}, {@link #mockBigTableRowKeyParser}, {@link #mockParsedMessage}) that
 * throw, and assert that each exception is mapped to the expected {@link ErrorType} on an invalid
 * record.</p>
 */
public class BigTableRecordParserTest {

    /** Stencil client for proto descriptors; replaced in setUp with a {@code CALLS_REAL_METHODS} mock. */
    @Mock
    private ClassLoadStencilClient stencilClient;
    /** Mock message parser used by error-path tests to simulate parse failures. */
    @Mock
    private MessageParser mockMessageParser;

    /** Mock row-key parser used to simulate row-key resolution failures. */
    @Mock
    private BigTableRowKeyParser mockBigTableRowKeyParser;
    /** Mock parsed message returned before a row-key failure is triggered. */
    @Mock
    private ParsedMessage mockParsedMessage;
    /** Parser under test, rebuilt per scenario from real or mocked collaborators. */
    private BigTableRecordParser bigTableRecordParser;
    /** Two valid booking-log messages forming the input batch. */
    private List<Message> messages;
    /** Sink configuration loaded from the seeded system properties. */
    private BigTableSinkConfig sinkConfig;

    /**
     * Seeds configuration and builds the record parser and message batch before each test.
     *
     * <p>Opens the Mockito annotations, sets the proto message class, {@code LOG_MESSAGE} mode, a
     * single-family column mapping and a constant row-key template as system properties, and builds
     * two booking-log {@link Message}s. It then constructs a real {@link ProtoMessageParser} (over a
     * {@code CALLS_REAL_METHODS} {@link ClassLoadStencilClient}), a {@link BigTableSchema}, a
     * {@link BigTableRowKeyParser} and the {@link BigTableRecordParser} under test.</p>
     *
     * @throws IOException if reading configuration or building the parser fails
     * @throws InvalidTemplateException if the row-key template is not valid
     */
    @Before
    public void setUp() throws IOException, InvalidTemplateException {
        MockitoAnnotations.openMocks(this);
        System.setProperty("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestBookingLogMessage");
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE", String.valueOf(SinkConnectorSchemaMessageMode.LOG_MESSAGE));
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\"} }");
        System.setProperty("SINK_BIGTABLE_ROW_KEY_TEMPLATE", "row-key-constant-string");


        TestBookingLogKey bookingLogKey1 = TestBookingLogKey.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1").build();
        TestBookingLogMessage bookingLogMessage1 = TestBookingLogMessage.newBuilder().setOrderNumber("order#1").setOrderUrl("order-url#1")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(100L).setNanos(200).build())
                .setServiceType(TestServiceType.Enum.GO_SEND)
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(100D).setLongitude(200D).build())
                .build();
        TestBookingLogKey bookingLogKey2 = TestBookingLogKey.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2").build();
        TestBookingLogMessage bookingLogMessage2 = TestBookingLogMessage.newBuilder().setOrderNumber("order#2").setOrderUrl("order-url#2")
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(101L).setNanos(202).build())
                .setServiceType(TestServiceType.Enum.GO_SHOP)
                .setDriverPickupLocation(TestLocation.newBuilder().setLatitude(300D).setLongitude(400D).build())
                .build();

        Message message1 = new Message(bookingLogKey1.toByteArray(), bookingLogMessage1.toByteArray());
        Message message2 = new Message(bookingLogKey2.toByteArray(), bookingLogMessage2.toByteArray());
        messages = Collections.list(message1, message2);

        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);

        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));

        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);
    }

    /**
     * Verifies that a batch of valid messages converts into valid, error-free records.
     *
     * <p>Given two well-formed booking-log messages and the default column mapping, when
     * {@link BigTableRecordParser#convert(java.util.List)} is called, then both resulting
     * {@link BigTableRecord}s are valid and carry no error info.</p>
     */
    @Test
    public void shouldReturnValidRecordsForListOfValidMessages() {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    /**
     * Verifies that a column mapping referencing a complex (message) field still produces valid records.
     *
     * <p>Given the column mapping extended with {@code q3 -> driver_pickup_location} (a nested message
     * field), when the batch is converted, then both records are valid with no error info.</p>
     *
     * @throws InvalidTemplateException if the row-key template is not valid
     */
    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForComplexFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location\"} }");
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);

        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    /**
     * Verifies that a column mapping referencing a nested timestamp field produces valid records.
     *
     * <p>Given the column mapping extended with {@code q3 -> event_timestamp.nanos}, when the batch is
     * converted, then both records are valid with no error info.</p>
     *
     * @throws InvalidTemplateException if the row-key template is not valid
     */
    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForNestedTimestampFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"event_timestamp.nanos\"} }");
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    /**
     * Verifies that a column mapping referencing a nested scalar field produces valid records.
     *
     * <p>Given the column mapping extended with {@code q3 -> driver_pickup_location.latitude}, when
     * the batch is converted, then both records are valid with no error info.</p>
     *
     * @throws InvalidTemplateException if the row-key template is not valid
     */
    @Test
    public void shouldReturnValidRecordsForListOfValidMessagesForNestedFieldsInColumnsMapping() throws InvalidTemplateException {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{ \"cf1\" : { \"q1\" : \"order_number\", \"q2\" : \"service_type\", \"q3\" : \"driver_pickup_location.latitude\"} }");
        sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        Configuration jsonPathConfig = Configuration.builder()
                .jsonProvider(new ProtoJsonProvider(sinkConfig))
                .build();
        ProtoMessageParser protoMessageParser = new ProtoMessageParser(stencilClient, jsonPathConfig);
        Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
        BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(new Template(sinkConfig.getRowKeyTemplate()));
        BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
        bigTableRecordParser = new BigTableRecordParser(protoMessageParser, bigTableRowKeyParser, modeAndSchema, bigtableSchema);

        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        assertTrue(records.get(0).isValid());
        assertTrue(records.get(1).isValid());
        assertNull(records.get(0).getErrorInfo());
        assertNull(records.get(1).getErrorInfo());
    }

    /**
     * Verifies that a message with null key and value is converted into an invalid record.
     *
     * <p>Given a single {@code Message(null, null)}, when the batch is converted, then the resulting
     * record is invalid and carries non-null error info.</p>
     */
    @Test
    public void shouldReturnInvalidRecordForAnyNullMessage() {
        List<BigTableRecord> records = bigTableRecordParser.convert(Collections.list(new Message(null, null)));
        assertFalse(records.get(0).isValid());
        assertNotNull(records.get(0).getErrorInfo());
    }

    /**
     * Verifies that an {@link EmptyMessageException} maps to an invalid-message error.
     *
     * <p>Given a mocked message parser that throws {@link EmptyMessageException}, when the batch is
     * converted, then every record is invalid with error type
     * {@link ErrorType#INVALID_MESSAGE_ERROR}.</p>
     *
     * @throws IOException declared by the mocked parser's {@code parse} method
     */
    @Test
    public void shouldCatchEmptyMessageExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsInvalidMessageError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(EmptyMessageException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.INVALID_MESSAGE_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    /**
     * Verifies that a {@link ConfigurationException} maps to an unknown-fields error.
     *
     * <p>Given a mocked message parser that throws {@link ConfigurationException}, when the batch is
     * converted, then every record is invalid with error type
     * {@link ErrorType#UNKNOWN_FIELDS_ERROR}.</p>
     *
     * @throws IOException declared by the mocked parser's {@code parse} method
     */
    @Test
    public void shouldCatchConfigurationExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(ConfigurationException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    /**
     * Verifies that an {@link IOException} maps to a deserialization error.
     *
     * <p>Given a mocked message parser that throws {@link IOException}, when the batch is converted,
     * then every record is invalid with error type {@link ErrorType#DESERIALIZATION_ERROR}.</p>
     *
     * @throws IOException declared by the mocked parser's {@code parse} method
     */
    @Test
    public void shouldCatchIOExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsDeserializationError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenThrow(IOException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.DESERIALIZATION_ERROR, record.getErrorInfo().getErrorType());
        }
    }

    /**
     * Verifies that an {@link IllegalArgumentException} during row-key parsing maps to an
     * unknown-fields error.
     *
     * <p>Given a mocked parser that returns {@link #mockParsedMessage} and a mocked row-key parser
     * whose {@code parse} throws {@link IllegalArgumentException}, when the batch is converted, then
     * every record is invalid with error type {@link ErrorType#UNKNOWN_FIELDS_ERROR}.</p>
     *
     * @throws IOException declared by the mocked parser's {@code parse} method
     */
    @Test
    public void shouldCatchIllegalArgumentExceptionAndReturnAnInvalidBigtableRecordWithErrorTypeAsUnknownFieldsError() throws IOException {
        bigTableRecordParser = new BigTableRecordParser(mockMessageParser,
                mockBigTableRowKeyParser,
                MessageConfigUtils.getModeAndSchema(sinkConfig),
                new BigTableSchema(sinkConfig.getColumnFamilyMapping())
        );
        when(mockMessageParser.parse(any(), any(), any())).thenReturn(mockParsedMessage);
        when(mockBigTableRowKeyParser.parse(mockParsedMessage)).thenThrow(IllegalArgumentException.class);

        List<BigTableRecord> bigTableRecords = bigTableRecordParser.convert(messages);

        for (BigTableRecord record : bigTableRecords) {
            assertFalse(record.isValid());
            assertEquals(ErrorType.UNKNOWN_FIELDS_ERROR, record.getErrorInfo().getErrorType());
        }
    }
}
