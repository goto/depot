package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TextMaxComputeTable;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import com.gotocompany.depot.maxcompute.enumeration.MaxComputeTimestampDataType;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategyFactory;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import org.assertj.core.groups.Tuple;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link MaxComputeSchemaBuilder}.
 *
 * <p>These tests verify that a MaxCompute table schema is assembled from the
 * {@link TextMaxComputeTable.Table} Protobuf descriptor, covering the three column groups produced by the
 * builder: data columns mapped from the Protobuf fields (including nested structs and arrays of structs),
 * metadata columns in either flat or namespaced layout, and a partition column when partitioning is enabled.
 * Configuration is supplied through a {@link MaxComputeSinkConfig} mock, partitioning is resolved with a real
 * {@link PartitioningStrategyFactory}, and the resulting columns are asserted with AssertJ by name and
 * {@code typeInfo}.</p>
 *
 * @see MaxComputeSchemaBuilder
 */
public class MaxComputeSchemaBuilderTest {

    /**
     * Protobuf descriptor of {@link TextMaxComputeTable.Table} from which each table schema is built.
     */
    private final Descriptors.Descriptor descriptor = TextMaxComputeTable.Table.getDescriptor();

    /**
     * Verifies that a partitioned schema includes flat (root-level) metadata columns and a partition column.
     *
     * <p>Given metadata enabled without a namespace and timestamp partitioning on {@code event_timestamp} into
     * the column {@code __partitioning_column}, when
     * {@link MaxComputeSchemaBuilder#build(Descriptors.Descriptor)} is invoked, then the table has seven
     * non-partition columns (the mapped data columns {@code id}, {@code user}, {@code items}, and
     * {@code event_timestamp} plus the three metadata columns {@code __message_timestamp}, {@code __kafka_topic},
     * and {@code __kafka_offset}) and one {@code STRING} partition column {@code __partitioning_column}, each
     * asserted by name and type.</p>
     */
    @Test
    public void shouldBuildPartitionedTableSchemaWithRootLevelMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy, new MetadataUtil(maxComputeSinkConfig));
        int expectedNonPartitionColumnCount = 7;
        int expectedPartitionColumnCount = 1;

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("__message_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("__kafka_topic", TypeInfoFactory.STRING),
                        Tuple.tuple("__kafka_offset", TypeInfoFactory.BIGINT)
                );
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    /**
     * Verifies that a partitioned schema nests metadata into a single namespaced struct column.
     *
     * <p>Given metadata enabled with the namespace {@code meta} and timestamp partitioning on
     * {@code event_timestamp}, when {@link MaxComputeSchemaBuilder#build(Descriptors.Descriptor)} is invoked,
     * then the table has five non-partition columns ({@code id}, {@code user}, {@code items},
     * {@code event_timestamp}, and the {@code meta} struct bundling the three metadata fields) and one
     * {@code STRING} partition column {@code __partitioning_column}, each asserted by name and type.</p>
     */
    @Test
    public void shouldBuildPartitionedTableSchemaWithNestedMetadata() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMaxcomputeMetadataNamespace()).thenReturn("meta");
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn("__partitioning_column");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        int expectedNonPartitionColumnCount = 5;
        int expectedPartitionColumnCount = 1;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy, new MetadataUtil(maxComputeSinkConfig));

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ),
                        Tuple.tuple("meta", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("__message_timestamp", "__kafka_topic", "__kafka_offset"),
                                Arrays.asList(TypeInfoFactory.TIMESTAMP_NTZ, TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)
                        ))
                );
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns())
                .extracting("name", "typeInfo")
                .contains(Tuple.tuple("__partitioning_column", TypeInfoFactory.STRING));
    }

    /**
     * Verifies that a plain schema is built when neither partitioning nor metadata is enabled.
     *
     * <p>Given metadata disabled and partitioning disabled, when
     * {@link MaxComputeSchemaBuilder#build(Descriptors.Descriptor)} is invoked, then the table has only the four
     * mapped data columns ({@code id}, {@code user}, {@code items}, and {@code event_timestamp}) and no
     * partition columns, each asserted by name and type.</p>
     */
    @Test
    public void shouldBuildTableSchemaWithoutPartitionAndMeta() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        int expectedNonPartitionColumnCount = 4;
        int expectedPartitionColumnCount = 0;
        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy, new MetadataUtil(maxComputeSinkConfig));

        MaxComputeSchema maxComputeSchema = maxComputeSchemaBuilder.build(descriptor);

        assertThat(maxComputeSchema.getTableSchema().getColumns().size()).isEqualTo(expectedNonPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getPartitionColumns().size()).isEqualTo(expectedPartitionColumnCount);
        assertThat(maxComputeSchema.getTableSchema().getColumns())
                .extracting("name", "typeInfo")
                .containsExactlyInAnyOrder(
                        Tuple.tuple("id", TypeInfoFactory.STRING),
                        Tuple.tuple("user", TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "contacts"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                        Arrays.asList("number"),
                                        Arrays.asList(TypeInfoFactory.STRING)
                                )))
                        )),
                        Tuple.tuple("items", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(
                                Arrays.asList("id", "name"),
                                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.STRING)
                        ))),
                        Tuple.tuple("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ)
                );
    }

    /**
     * Verifies that a partition key missing from the descriptor causes schema construction to fail.
     *
     * <p>Given partitioning enabled with the partition key {@code non_existent_partition_key}, which the
     * {@link TextMaxComputeTable.Table} descriptor does not declare, when the partitioning strategy is created
     * and the builder is exercised, then an {@link IllegalArgumentException} is thrown (raised by
     * {@link PartitioningStrategyFactory} while resolving the partition key), as asserted by the
     * {@code expected} attribute of the {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenPartitionKeyIsNotFound() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.shouldAddMetadata()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getMetadataColumnsTypes()).thenReturn(
                Arrays.asList(new TupleString("__message_timestamp", "timestamp"),
                        new TupleString("__kafka_topic", "string"),
                        new TupleString("__kafka_offset", "long")
                )
        );
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("non_existent_partition_key");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxComputeProtoTimestampToMaxcomputeType()).thenReturn(MaxComputeTimestampDataType.TIMESTAMP_NTZ);
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
        MaxComputeSchemaBuilder maxComputeSchemaBuilder = new MaxComputeSchemaBuilder(new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig, partitioningStrategy, new MetadataUtil(maxComputeSinkConfig));

        maxComputeSchemaBuilder.build(descriptor);
    }

}
