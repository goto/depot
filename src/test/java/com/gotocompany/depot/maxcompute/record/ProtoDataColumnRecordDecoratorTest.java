package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaBuilder;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.schema.partition.DefaultPartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.TimestampPartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProtoDataColumnRecordDecorator}.
 *
 * <p>These tests verify that the Protobuf payload of a Depot {@link Message} is mapped onto the data columns of
 * a MaxCompute {@link RecordWrapper}, that nested repeated messages become arrays of structs, that timestamps
 * are converted to UTC {@link LocalDateTime} values, and that the partition column is handled according to the
 * active {@link PartitioningStrategy}. A real {@link MaxComputeSchemaBuilder} and
 * {@link ProtobufConverterOrchestrator} are used so the assertions exercise the production schema and
 * conversion logic, while the {@link MaxComputeSchemaCache} and {@code ProtoMessageParser} are mocked: the
 * parser is stubbed to return a {@link ParsedMessage} whose raw payload is the supplied
 * {@link TestMaxComputeRecord.MaxComputeRecord}.</p>
 *
 * <p>The scenarios cover plain data-column mapping, omission of the original column when partitioning by a
 * primitive value via {@link DefaultPartitioningStrategy}, retention of the original column when partitioning by
 * timestamp via {@link TimestampPartitioningStrategy}, the default {@code __NULL__} partition spec when the
 * partition field is absent, and delegation to an injected downstream {@link RecordDecorator}.</p>
 *
 * @see ProtoDataColumnRecordDecorator
 */
public class ProtoDataColumnRecordDecoratorTest {

    /**
     * Protobuf descriptor of {@link TestMaxComputeRecord.MaxComputeRecord} from which the MaxCompute schema is
     * built in every test.
     */
    private static final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    /**
     * Real schema builder, recreated by {@link #instantiateProtoDataColumnRecordDecorator} and used to produce
     * the {@link MaxComputeSchema} whose table schema backs the records under test.
     */
    private MaxComputeSchemaBuilder maxComputeSchemaBuilder;
    /**
     * The decorator under test, recreated per scenario by
     * {@link #instantiateProtoDataColumnRecordDecorator}.
     */
    private ProtoDataColumnRecordDecorator protoDataColumnRecordDecorator;

    /**
     * Builds the default fixture before each test.
     *
     * <p>Mocks a non-partitioned {@link MaxComputeSinkConfig} with metadata disabled, a UTC zone, permissive
     * timestamp bounds, and {@link MaxComputeTimestampDataType#TIMESTAMP_NTZ} timestamps, mocks a
     * {@link SinkConfig} in {@link SinkConnectorSchemaMessageMode#LOG_MESSAGE} mode, and delegates to
     * {@link #instantiateProtoDataColumnRecordDecorator} with no downstream decorator and no partitioning
     * strategy.</p>
     *
     * @throws IOException never in this test; declared because decorator construction parses a message
     */
    @Before
    public void setup() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, null, getMockedMessage());
    }

    /**
     * Verifies that the Protobuf payload is mapped onto the data columns of a non-partitioned record.
     *
     * <p>Given the default non-partitioned fixture and a {@link Message} built from the sample
     * {@link TestMaxComputeRecord.MaxComputeRecord}, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the decorated
     * record's values are asserted to equal the id {@code "id"}, the {@code inner_record} repeated field mapped
     * to a list of two {@link SimpleStruct} elements, the timestamp converted to a UTC {@link LocalDateTime},
     * and a trailing {@code null} for the unset field.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldProcessDataColumnToRecord() throws IOException {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        expectedLocalDateTime,
                        null});
    }

    /**
     * Verifies that the original column is omitted from the data columns when partitioning by a primitive value.
     *
     * <p>Given a partitioned {@link MaxComputeSinkConfig} whose partition key and partition column name are both
     * {@code id}, paired with a {@link DefaultPartitioningStrategy} over a {@code STRING} type, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the {@code id}
     * field is treated as the replacing partition column and excluded from the data columns; the decorated
     * record's values are asserted to begin with the {@code inner_record} list, followed by the timestamp and a
     * trailing {@code null}.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldProcessDataColumnToRecordAndOmitPartitionColumnIfPartitionedByPrimitiveTypes() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("id");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("id");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                maxComputeSinkConfig);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        expectedLocalDateTime,
                        null});
    }

    /**
     * Verifies that the original column is retained when partitioning by a timestamp into a separate partition
     * column.
     *
     * <p>Given a partitioned {@link MaxComputeSinkConfig} whose partition key is {@code timestamp} but whose
     * partition column name is the distinct {@code __partition_key} (day granularity), paired with a
     * {@link TimestampPartitioningStrategy}, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the original
     * {@code timestamp} column is not omitted; the decorated record's values are asserted to equal the id, the
     * {@code inner_record} list, the timestamp converted to a UTC {@link LocalDateTime}, and a trailing
     * {@code null}.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldProcessDataColumnToRecordAndShouldNotOmitOriginalColumnIfPartitionedByTimestamp() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_key");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new TimestampPartitioningStrategy(maxComputeSinkConfig);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        "id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        expectedLocalDateTime,
                        null});
    }

    /**
     * Verifies that the default partition spec is produced when the partition field is absent from the payload.
     *
     * <p>Given a {@link TimestampPartitioningStrategy} over the {@code timestamp} field but a sample
     * {@link TestMaxComputeRecord.MaxComputeRecord} that does not set a timestamp, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the timestamp
     * data column is left {@code null} and the resulting partition spec is asserted to be the default
     * {@code __partition_key='__NULL__'}.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldSetDefaultPartitioningSpecWhenProtoFieldNotExists() throws IOException {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partition_key");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(100);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(100);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        PartitioningStrategy partitioningStrategy = new TimestampPartitioningStrategy(maxComputeSinkConfig);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = TestMaxComputeRecord.MaxComputeRecord
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
                .build();
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, null, partitioningStrategy, maxComputeRecord);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        Message message = new Message(null, maxComputeRecord.toByteArray());
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedWrapper =
                protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{
                        "id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        null,
                        null});
        assertThat(decoratedWrapper.getPartitionSpec().toString())
                .isEqualTo("__partition_key='__NULL__'");
    }

    /**
     * Verifies that data columns are populated correctly under the default non-partitioned fixture.
     *
     * <p>Given the default fixture in which no {@link PartitioningStrategy} is configured, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked on a {@link Message}
     * built from the sample record, then the decorated record's values are asserted to equal the id, the
     * {@code inner_record} list of two {@link SimpleStruct} elements, the timestamp converted to a UTC
     * {@link LocalDateTime}, and a trailing {@code null}.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldPutDefaultPartitionSpec() throws IOException {
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC
        );
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecordWrapper = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecordWrapper.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        expectedLocalDateTime,
                        null});
    }

    /**
     * Verifies that an injected downstream decorator is invoked as part of the decoration chain.
     *
     * <p>Given a mocked downstream {@link RecordDecorator} stubbed to echo back its input wrapper and injected
     * as this decorator's nested link, when
     * {@link ProtoDataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the data columns
     * are populated as in the plain case and the injected decorator's {@code decorate} method is verified to be
     * called exactly once.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void decorateShouldCallInjectedDecorator() throws IOException {
        RecordDecorator recordDecorator = Mockito.mock(RecordDecorator.class);
        when(recordDecorator.decorate(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        SinkConfig sinkConfig = Mockito.mock(SinkConfig.class);
        when(sinkConfig.getSinkConnectorSchemaMessageMode()).thenReturn(SinkConnectorSchemaMessageMode.LOG_MESSAGE);
        instantiateProtoDataColumnRecordDecorator(sinkConfig, maxComputeSinkConfig, recordDecorator, null, getMockedMessage());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        Record record = new ArrayRecord(maxComputeSchema.getTableSchema());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        TestMaxComputeRecord.MaxComputeRecord maxComputeRecord = getMockedMessage();
        Message message = new Message(null, maxComputeRecord.toByteArray());
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                10002010L,
                1000,
                java.time.ZoneOffset.UTC);
        StructTypeInfo expectedArrayStructElementTypeInfo = (StructTypeInfo) ((ArrayTypeInfo) getDataColumnTypeByName(maxComputeSchema.getTableSchema(), "inner_record")).getElementTypeInfo();

        RecordWrapper decoratedRecord = protoDataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(decoratedRecord.getRecord())
                .extracting("values")
                .isEqualTo(new Object[]{"id",
                        Arrays.asList(
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_1", 100.2f, null)),
                                new SimpleStruct(expectedArrayStructElementTypeInfo, Arrays.asList("name_2", 50f, null))
                        ),
                        expectedLocalDateTime,
                        null});
        verify(recordDecorator, Mockito.times(1))
                .decorate(Mockito.any(), Mockito.any());
    }

    /**
     * Builds the schema, mocks, and decorator used by a single scenario.
     *
     * <p>Creates a real {@link ProtobufConverterOrchestrator} and {@link MaxComputeSchemaBuilder} (the latter
     * stored in {@link #maxComputeSchemaBuilder}), builds a {@link MaxComputeSchema} from {@link #DESCRIPTOR},
     * mocks a {@link MaxComputeSchemaCache} returning that schema, and stubs a {@code ProtoMessageParser} to
     * return a {@link ParsedMessage} whose raw payload is the supplied Protobuf message. It then constructs the
     * {@link ProtoDataColumnRecordDecorator} under test with the given downstream decorator and partitioning
     * strategy, a mocked {@link StatsDReporter}, and a real {@link MaxComputeMetrics}.</p>
     *
     * @param sinkConfig           the general sink configuration providing the schema message mode
     * @param maxComputeSinkConfig the MaxCompute sink configuration driving schema and partition handling
     * @param recordDecorator      the downstream decorator to inject, or {@code null} for none
     * @param partitioningStrategy the partitioning strategy to apply, or {@code null} when not partitioned
     * @param mockedMessage        the Protobuf message returned by the stubbed parser as the parsed payload
     * @throws IOException declared because the stubbed parser and decorator construction may throw it
     */
    private void instantiateProtoDataColumnRecordDecorator(SinkConfig sinkConfig, MaxComputeSinkConfig maxComputeSinkConfig,
                                                           RecordDecorator recordDecorator,
                                                           PartitioningStrategy partitioningStrategy,
                                                           com.google.protobuf.Message mockedMessage) throws IOException {
        ProtobufConverterOrchestrator protobufConverterOrchestrator = new ProtobufConverterOrchestrator(maxComputeSinkConfig);
        maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(
                protobufConverterOrchestrator,
                maxComputeSinkConfig,
                partitioningStrategy,
                new MetadataUtil(maxComputeSinkConfig)
        );
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(DESCRIPTOR);
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        ProtoMessageParser protoMessageParser = Mockito.mock(ProtoMessageParser.class);
        ParsedMessage parsedMessage = Mockito.mock(ParsedMessage.class);
        when(parsedMessage.getRaw()).thenReturn(mockedMessage);
        when(protoMessageParser.parse(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(parsedMessage);
        MaxComputeMetrics maxComputeMetrics = new MaxComputeMetrics(sinkConfig);
        protoDataColumnRecordDecorator = new ProtoDataColumnRecordDecorator(
                recordDecorator,
                protobufConverterOrchestrator,
                protoMessageParser,
                sinkConfig,
                partitioningStrategy,
                Mockito.mock(StatsDReporter.class),
                maxComputeMetrics
        );
    }

    /**
     * Looks up the declared {@link TypeInfo} of a column by name within the given table schema.
     *
     * <p>Used by the assertions to obtain the expected struct element type of the {@code inner_record} array
     * column.</p>
     *
     * @param tableSchema the table schema to search
     * @param columnName  the name of the column whose type is requested
     * @return the column's {@link TypeInfo}, or {@code null} if no column with that name exists
     */
    private static TypeInfo getDataColumnTypeByName(TableSchema tableSchema, String columnName) {
        return tableSchema.getColumns()
                .stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst()
                .map(com.aliyun.odps.Column::getTypeInfo)
                .orElse(null);
    }

    /**
     * Builds the sample {@link TestMaxComputeRecord.MaxComputeRecord} shared by most scenarios.
     *
     * <p>The message carries an id, two inner records, and a Protobuf {@link Timestamp} of {@code 10002010}
     * seconds and {@code 1000} nanos, which the tests expect to be converted into a UTC
     * {@link LocalDateTime}.</p>
     *
     * @return a populated {@link TestMaxComputeRecord.MaxComputeRecord} instance
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
