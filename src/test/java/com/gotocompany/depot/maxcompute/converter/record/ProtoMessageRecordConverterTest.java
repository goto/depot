package com.gotocompany.depot.maxcompute.converter.record;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.exceptions.SchemaMismatchException;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaBuilder;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.model.RecordWrappers;
import com.gotocompany.depot.maxcompute.record.ProtoDataColumnRecordDecorator;
import com.gotocompany.depot.maxcompute.record.ProtoMetadataColumnRecordDecorator;
import com.gotocompany.depot.maxcompute.record.RecordDecorator;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProtoMessageRecordConverter}, which turns a batch of {@link Message}s into
 * {@link RecordWrappers}, separating successfully converted records from invalid ones.
 *
 * <p>The converter delegates each message to a {@link RecordDecorator} chain and classifies any failure into
 * an {@link ErrorInfo} with the appropriate {@link ErrorType}. {@link #setup()} wires up a realistic chain
 * (data and metadata decorators, a schema built by {@link MaxComputeSchemaBuilder}, and a partitioning
 * strategy) over a Mockito-mocked {@link MaxComputeSinkConfig} that enables metadata columns, timestamp
 * partitioning, and {@code TIMESTAMP_NTZ} timestamps.</p>
 *
 * <p>The happy-path test asserts the full set of values placed on the resulting record (metadata columns plus
 * the converted payload, including a nested struct list). The remaining tests substitute a mocked
 * {@link RecordDecorator} that throws a specific exception and assert the corresponding {@link ErrorType}
 * classification: {@code DESERIALIZATION_ERROR}, {@code UNKNOWN_FIELDS_ERROR}, {@code SINK_NON_RETRYABLE_ERROR},
 * and {@code INVALID_MESSAGE_ERROR}.</p>
 */
public class ProtoMessageRecordConverterTest {

    /**
     * Descriptor of the {@code MaxComputeRecord} fixture message that the converter is built around.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    /**
     * Mocked sink configuration controlling metadata columns, partitioning, and timestamp handling.
     */
    private MaxComputeSinkConfig maxComputeSinkConfig;

    /**
     * Real orchestrator that maps the message's Protobuf fields to MaxCompute types and values.
     */
    private ProtobufConverterOrchestrator protobufConverterOrchestrator;

    /**
     * Mocked Protobuf message parser stubbed to return the sample parsed message.
     */
    private ProtoMessageParser protoMessageParser;

    /**
     * Real schema builder that derives the MaxCompute schema for the record descriptor.
     */
    private MaxComputeSchemaBuilder maxComputeSchemaBuilder;

    /**
     * Mocked sink configuration used for metrics and the schema-message-mode lookup.
     */
    private SinkConfig sinkConfig;

    /**
     * Mocked schema cache stubbed to return the schema built in {@link #setup()}.
     */
    private MaxComputeSchemaCache maxComputeSchemaCache;

    /**
     * The converter under test, assembled over the decorator chain in {@link #setup()}.
     */
    private ProtoMessageRecordConverter protoMessageRecordConverter;

    /**
     * Wires up the converter under test and its supporting collaborators.
     *
     * <p>Stubs a {@link MaxComputeSinkConfig} that enables metadata columns (message timestamp, Kafka topic,
     * and Kafka offset), timestamp partitioning on the {@code timestamp} field, a UTC zone, a permissive
     * valid-timestamp range, and {@code TIMESTAMP_NTZ} timestamps. Builds a real
     * {@link ProtobufConverterOrchestrator}, partitioning strategy, and {@link MaxComputeSchemaBuilder}, stubs
     * the {@link MaxComputeSchemaCache} to return the built schema, and assembles the data and metadata
     * {@link RecordDecorator}s that back the {@link ProtoMessageRecordConverter}.</p>
     *
     * @throws IOException if building the schema or parsing the mocked message fails
     */
    @Before
    public void setup() throws IOException {
        maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_column");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        protobufConverterOrchestrator = new ProtobufConverterOrchestrator(maxComputeSinkConfig);
        protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        ParsedMessage parsedMessage = Mockito.mock(ParsedMessage.class);
        when(parsedMessage.getRaw()).thenReturn(getMockedMessage());
        when(protoMessageParser.parse(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(parsedMessage);
        sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode())
                .thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                protobufConverterOrchestrator,
                maxComputeSinkConfig,
                descriptor
        );
        MetadataUtil metadataUtil = new MetadataUtil(maxComputeSinkConfig);
        maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(protobufConverterOrchestrator, maxComputeSinkConfig, partitioningStrategy, metadataUtil);
        maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);
        when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing().when(instrumentation)
                .captureDurationSince(Mockito.any(), Mockito.any());
        MaxComputeMetrics maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        RecordDecorator protoDataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(null,
                protobufConverterOrchestrator,
                protoMessageParser, sinkConfig, partitioningStrategy, Mockito.mock(StatsDReporter.class), maxComputeMetrics);
        RecordDecorator metadataColumnRecordDecorator = new ProtoMetadataColumnRecordDecorator(protoDataColumnRecordDecorator, maxComputeSinkConfig, maxComputeSchemaCache, metadataUtil);
        protoMessageRecordConverter = new ProtoMessageRecordConverter(metadataColumnRecordDecorator, maxComputeSchemaCache);
    }

    /**
     * Verifies that a valid message is converted into a single valid record wrapper.
     *
     * <p>Builds a {@link Message} with metadata tuples (message timestamp, Kafka topic, and Kafka offset) and
     * the mocked payload, converts it with {@link ProtoMessageRecordConverter#convert(java.util.List)}, and
     * asserts there is exactly one valid record at index {@code 0} with no error. Checks that the record's
     * values contain the expected partition timestamp, topic, offset, id, the nested inner-record struct list,
     * and the payload timestamp.</p>
     */
    @Test
    public void shouldConvertMessageToRecordWrapper() {
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        LocalDateTime expectedTimestampLocalDateTime = Instant.ofEpochMilli(
                        123012311L).atZone(ZoneId.of("UTC"))
                .toLocalDateTime();
        LocalDateTime expectedPayloadLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                ZoneOffset.UTC
        );

        RecordWrappers recordWrappers = protoMessageRecordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getValidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getValidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        StructTypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("name", "balance", "unset_string_nested"),
                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.FLOAT, TypeInfoFactory.STRING)
        );
        assertThat(recordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Serializable[]{
                        expectedTimestampLocalDateTime,
                        "topic",
                        100L,
                        "id",
                        new ArrayList<>(Arrays.asList(
                                new SimpleStruct(
                                        structTypeInfo,
                                        Arrays.asList("name_1", 100.2f, null)
                                ),
                                new SimpleStruct(
                                        structTypeInfo,
                                        Arrays.asList("name_2", 50f, null)
                                )
                        )),
                        expectedPayloadLocalDateTime,
                        null
                });
        assertThat(recordWrapper.getErrorInfo()).isNull();
    }

    /**
     * Verifies that an {@link IOException} from the decorator yields a deserialization error.
     *
     * <p>Replaces the decorator with a mock that throws {@link IOException} during decoration, converts a
     * single message, and asserts there is one invalid record at index {@code 0} with a {@code null} record and
     * an {@link ErrorInfo} of type {@code DESERIALIZATION_ERROR}.</p>
     *
     * @throws IOException if stubbing the decorator's {@code decorate} method requires it
     */
    @Test
    public void shouldReturnRecordWrapperWithDeserializationErrorWhenIOExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        Mockito.doThrow(new IOException()).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        assertThat(recordWrapper.getRecord())
                .isNull();
        assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new IOException(), ErrorType.DESERIALIZATION_ERROR));
    }

    /**
     * Verifies that an {@link UnknownFieldsException} from the decorator yields an unknown-fields error.
     *
     * <p>Replaces the decorator with a mock that throws {@link UnknownFieldsException} during decoration,
     * converts a single message, and asserts there is one invalid record at index {@code 0} with a
     * {@code null} record and an {@link ErrorInfo} of type {@code UNKNOWN_FIELDS_ERROR}.</p>
     *
     * @throws IOException if stubbing the decorator's {@code decorate} method requires it
     */
    @Test
    public void shouldReturnRecordWrapperWithUnknownFieldsErrorWhenUnknownFieldExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        com.google.protobuf.Message mockedMessage = getMockedMessage();
        Mockito.doThrow(new UnknownFieldsException(mockedMessage)).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        assertThat(recordWrapper.getRecord())
                .isNull();
        assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new UnknownFieldsException(mockedMessage), ErrorType.UNKNOWN_FIELDS_ERROR));
    }

    /**
     * Verifies that a {@link SchemaMismatchException} from the decorator yields a non-retryable error.
     *
     * <p>Replaces the decorator with a mock that throws a {@link SchemaMismatchException} during decoration,
     * converts a single message, and asserts there is one invalid record at index {@code 0} with a
     * {@code null} record and an {@link ErrorInfo} of type {@code SINK_NON_RETRYABLE_ERROR}.</p>
     *
     * @throws IOException if stubbing the decorator's {@code decorate} method requires it
     */
    @Test
    public void shouldReturnRecordWrapperWithNonRetryableErrorWhenUnknownFieldExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        com.google.protobuf.Message mockedMessage = getMockedMessage();
        Mockito.doThrow(new SchemaMismatchException("Schema Mismatch", "v1")).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        assertThat(recordWrapper.getRecord())
                .isNull();
        assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new UnknownFieldsException(mockedMessage), ErrorType.SINK_NON_RETRYABLE_ERROR));
    }

    /**
     * Verifies that an {@link InvalidMessageException} from the decorator yields an invalid-message error.
     *
     * <p>Replaces the decorator with a mock that throws {@link InvalidMessageException} during decoration,
     * converts a single message, and asserts there is one invalid record at index {@code 0} with a
     * {@code null} record and an {@link ErrorInfo} of type {@code INVALID_MESSAGE_ERROR}.</p>
     *
     * @throws IOException if stubbing the decorator's {@code decorate} method requires it
     */
    @Test
    public void shouldReturnRecordWrapperWithInvalidMessageErrorWhenInvalidMessageExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        String invalidMessage = "Invalid message";
        Mockito.doThrow(new InvalidMessageException(invalidMessage)).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        assertThat(recordWrapper.getRecord())
                .isNull();
        assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new InvalidMessageException(invalidMessage), ErrorType.INVALID_MESSAGE_ERROR));
    }

    /**
     * Verifies that an {@link EmptyMessageException} from the decorator yields an invalid-message error.
     *
     * <p>Replaces the decorator with a mock that throws {@link EmptyMessageException} during decoration,
     * converts a single message, and asserts there is one invalid record at index {@code 0} with a
     * {@code null} record and an {@link ErrorInfo} of type {@code INVALID_MESSAGE_ERROR}.</p>
     *
     * @throws IOException if stubbing the decorator's {@code decorate} method requires it
     */
    @Test
    public void shouldReturnRecordWrapperWithInvalidMessageErrorWhenInvalidEmptyMessageExceptionIsThrown() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        String invalidMessage = "Invalid message";
        Mockito.doThrow(new EmptyMessageException()).when(recordDecorator)
                .decorate(Mockito.any(), Mockito.any());
        ProtoMessageRecordConverter recordConverter = new ProtoMessageRecordConverter(recordDecorator, maxComputeSchemaCache);
        Message message = new Message(
                null,
                getMockedMessage().toByteArray(),
                new Tuple<>("__message_timestamp", 123012311L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );

        RecordWrappers recordWrappers = recordConverter.convert(Collections.singletonList(message));

        assertThat(recordWrappers.getInvalidRecords()).size().isEqualTo(1);
        RecordWrapper recordWrapper = recordWrappers.getInvalidRecords().get(0);
        assertThat(recordWrapper.getIndex()).isEqualTo(0);
        assertThat(recordWrapper.getRecord())
                .isNull();
        assertThat(recordWrapper.getErrorInfo())
                .isEqualTo(new ErrorInfo(new InvalidMessageException(invalidMessage), ErrorType.INVALID_MESSAGE_ERROR));
    }

    /**
     * Builds the sample {@code MaxComputeRecord} payload shared by the tests.
     *
     * <p>Constructs a record with an id, two inner records (each carrying a name and balance), and a timestamp,
     * used both to stub the parsed message and to populate the {@link Message} payloads under test.</p>
     *
     * @return a populated {@code TestMaxComputeRecord.MaxComputeRecord} instance
     */
    private static TestMaxComputeRecord.MaxComputeRecord getMockedMessage() {
        return TestMaxComputeRecord.MaxComputeRecord
                .newBuilder()
                .setId("id")
                .addAllInnerRecord(Arrays.asList(
                        TestMaxComputeRecord.InnerRecord.newBuilder()
                                .setName("name_1")
                                .setBalance(100.2f)
                                .build(),
                        TestMaxComputeRecord.InnerRecord.newBuilder()
                                .setName("name_2")
                                .setBalance(50f)
                                .build()
                ))
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(10002010)
                        .setNanos(1000)
                        .build())
                .build();
    }

}
