package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeRecord;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaBuilder;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.proto.ProtoParsedMessage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProtoMetadataColumnRecordDecorator}.
 *
 * <p>These tests verify that Depot message metadata is appended to a MaxCompute record in both supported
 * layouts: nested inside a single namespaced {@code STRUCT} column, and flattened into individual top-level
 * columns. A real schema is built from the {@link TestMaxComputeRecord.MaxComputeRecord} Protobuf descriptor
 * through a {@link MaxComputeSchemaBuilder}, and the {@link MaxComputeSchemaCache} is mocked to return that
 * schema. The {@link MaxComputeSinkConfig} is mocked to declare three metadata columns
 * ({@code __message_timestamp}, {@code __kafka_topic}, and {@code __kafka_offset}) and a UTC zone, while a real
 * {@link MetadataUtil} performs the value coercion so the assertions exercise the production conversion
 * logic.</p>
 *
 * @see ProtoMetadataColumnRecordDecorator
 */
public class ProtoMetadataColumnRecordDecoratorTest {

    /**
     * Protobuf descriptor of {@link TestMaxComputeRecord.MaxComputeRecord} used to build the MaxCompute schema
     * exercised by the tests.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputeRecord.MaxComputeRecord.getDescriptor();

    /**
     * The MaxCompute sink configuration active for the current test; reassigned by
     * {@link #initializeDecorator(MaxComputeSinkConfig)} so assertions can read the configured namespace back.
     */
    private MaxComputeSinkConfig maxComputeSinkConfig;
    /**
     * Mocked schema cache that returns the schema built from {@link #descriptor}, serving as the source of the
     * metadata column types.
     */
    private MaxComputeSchemaCache maxComputeSchemaCache;
    /**
     * The decorator under test, recreated per configuration by
     * {@link #initializeDecorator(MaxComputeSinkConfig)}.
     */
    private ProtoMetadataColumnRecordDecorator protoMetadataColumnRecordDecorator;

    /**
     * Builds the default fixture before each test.
     *
     * <p>Mocks a {@link MaxComputeSinkConfig} with metadata enabled, a {@code __kafka_metadata} namespace, the
     * three metadata columns, a UTC zone, and {@link MaxComputeTimestampDataType#TIMESTAMP_NTZ} timestamps, then
     * delegates to {@link #initializeDecorator(MaxComputeSinkConfig)} to construct the schema, cache, and
     * decorator.</p>
     */
    @Before
    public void setup() {
        MaxComputeSinkConfig config = Mockito.mock(MaxComputeSinkConfig.class);
        when(config.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(config.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(config.getMaxcomputeMetadataNamespace()).thenReturn("__kafka_metadata");
        when(config.getMetadataColumnsTypes()).thenReturn(Arrays.asList(
                new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        ));
        when(config.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(config.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(config.getMaxNestedMessageDepth()).thenReturn(15);
        initializeDecorator(config);
    }

    /**
     * Verifies that metadata is written into a single namespaced struct column when a metadata namespace is
     * configured.
     *
     * <p>Given the default fixture (namespace {@code __kafka_metadata}) and a {@link Message} carrying a message
     * timestamp of {@code 10002010L}, the Kafka topic {@code topic}, and the Kafka offset {@code 100L}, when
     * {@link ProtoMetadataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the record's
     * namespace column is asserted to equal a {@link SimpleStruct} containing the timestamp converted to a UTC
     * {@link LocalDateTime}, the topic string, and the offset as a {@code BIGINT}.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void shouldPopulateRecordWithNamespacedMetadata() throws IOException {
        Message message = new Message(
                null,
                new ProtoParsedMessage(getMockedMessage(), null),
                new Tuple<>("__message_timestamp", 10002010L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        Record record = new ArrayRecord(maxComputeSchemaCache.getMaxComputeSchema().getColumns());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        LocalDateTime expectedLocalDateTime = Instant.ofEpochMilli(10002010L)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();

        protoMetadataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(record.get(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()))
                .isEqualTo(new SimpleStruct(
                        TypeInfoFactory.getStructTypeInfo(Arrays.asList("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                Arrays.asList(TypeInfoFactory.TIMESTAMP, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)),
                        Arrays.asList(expectedLocalDateTime, "topic", 100L)
                ));
    }

    /**
     * Verifies that metadata is written into individual top-level columns when no metadata namespace is
     * configured.
     *
     * <p>Given a reconfigured {@link MaxComputeSinkConfig} that enables metadata but leaves the namespace unset,
     * and a {@link Message} carrying a message timestamp, Kafka topic, and Kafka offset, when
     * {@link ProtoMetadataColumnRecordDecorator#decorate(RecordWrapper, Message)} is invoked, then the
     * individual columns {@code __message_timestamp}, {@code __kafka_topic}, and {@code __kafka_offset} are
     * asserted to hold the UTC {@link LocalDateTime}, the topic string, and the offset respectively.</p>
     *
     * @throws IOException never in this test; declared because the decoration path may throw it
     */
    @Test
    public void shouldPopulateRecordWithNonNamespacedMetadata() throws IOException {
        MaxComputeSinkConfig mcSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(mcSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(mcSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(mcSinkConfig.getMetadataColumnsTypes()).thenReturn(Arrays.asList(
                new TupleString("__message_timestamp", "timestamp"),
                new TupleString("__kafka_topic", "string"),
                new TupleString("__kafka_offset", "long")
        ));
        when(mcSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(mcSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(mcSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);
        initializeDecorator(mcSinkConfig);
        Message message = new Message(
                null,
                new ProtoParsedMessage(getMockedMessage(), null),
                new Tuple<>("__message_timestamp", 10002010L),
                new Tuple<>("__kafka_topic", "topic"),
                new Tuple<>("__kafka_offset", 100L)
        );
        Record record = new ArrayRecord(maxComputeSchemaCache.getMaxComputeSchema().getColumns());
        RecordWrapper recordWrapper = new RecordWrapper(record, 0, null, null);
        LocalDateTime expectedLocalDateTime = Instant.ofEpochMilli(10002010L)
                .atZone(ZoneId.of("UTC"))
                .toLocalDateTime();

        protoMetadataColumnRecordDecorator.decorate(recordWrapper, message);

        assertThat(record)
                .satisfies(r -> {
                    assertThat(r.get("__message_timestamp"))
                            .isEqualTo(expectedLocalDateTime);
                    assertThat(r.get("__kafka_topic"))
                            .isEqualTo("topic");
                    assertThat(r.get("__kafka_offset"))
                            .isEqualTo(100L);
                });
    }

    /**
     * Builds the sample {@link TestMaxComputeRecord.MaxComputeRecord} used as the Protobuf payload in the tests.
     *
     * <p>The message carries an id, two inner records, and a Protobuf {@link Timestamp}; because only the
     * appended metadata is asserted, the payload mainly serves to provide a valid parsed message.</p>
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

    /**
     * (Re)builds the schema, schema cache, and decorator from the supplied configuration.
     *
     * <p>Stores the configuration in {@link #maxComputeSinkConfig}, builds a {@link MaxComputeSchema} from
     * {@link #descriptor} using a real {@link MaxComputeSchemaBuilder} and {@link MetadataUtil}, mocks a
     * {@link MaxComputeSchemaCache} to return that schema, and constructs the
     * {@link ProtoMetadataColumnRecordDecorator} under test with no downstream decorator.</p>
     *
     * @param sinkConfig the MaxCompute sink configuration that drives schema construction and metadata layout
     */
    private void initializeDecorator(MaxComputeSinkConfig sinkConfig) {
        this.maxComputeSinkConfig = sinkConfig;
        ProtobufConverterOrchestrator protobufConverterOrchestrator = new ProtobufConverterOrchestrator(sinkConfig);
        MetadataUtil metadataUtil = new MetadataUtil(maxComputeSinkConfig);
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(protobufConverterOrchestrator, sinkConfig, null, metadataUtil);
        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);
        maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        when(maxComputeSchemaCache.getMaxComputeSchema()).thenReturn(maxComputeSchema);
        protoMetadataColumnRecordDecorator = new ProtoMetadataColumnRecordDecorator(null, sinkConfig, maxComputeSchemaCache, metadataUtil);
    }
}
