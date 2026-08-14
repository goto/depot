package com.gotocompany.depot.maxcompute.schema.partition;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputePartition;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitioningStrategyFactory}.
 *
 * <p>These tests verify that the factory selects the correct {@link PartitioningStrategy} from the partition
 * key's resolved MaxCompute type, returns {@code null} when partitioning is disabled, and rejects unsupported
 * or missing partition keys. The partition key field is resolved from the
 * {@link TestMaxComputePartition.MaxComputePartition} descriptor through a real
 * {@link ProtobufConverterOrchestrator}, while {@link MaxComputeSinkConfig} is mocked to choose the partition
 * key and whether partitioning is enabled.</p>
 *
 * @see PartitioningStrategyFactory
 */
public class PartitioningStrategyFactoryTest {

    /**
     * Protobuf descriptor of {@link TestMaxComputePartition.MaxComputePartition}, whose fields serve as the
     * candidate partition keys in each scenario.
     */
    private final Descriptors.Descriptor descriptor = TestMaxComputePartition.MaxComputePartition.getDescriptor();

    /**
     * Verifies that a string partition key yields a {@link DefaultPartitioningStrategy}.
     *
     * <p>Given partitioning enabled with the partition key {@code string_field}, when
     * {@link PartitioningStrategyFactory#createPartitioningStrategy(ProtobufConverterOrchestrator, MaxComputeSinkConfig, Descriptors.Descriptor)}
     * is invoked, then the returned strategy is asserted to be a {@link DefaultPartitioningStrategy}.</p>
     */
    @Test
    public void shouldReturnDefaultPartitionStrategy() {
        String stringFieldName = "string_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(stringFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(stringFieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);


        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        assertTrue(partitioningStrategy instanceof DefaultPartitioningStrategy);
    }

    /**
     * Verifies that a timestamp partition key yields a {@link TimestampPartitioningStrategy}.
     *
     * <p>Given partitioning enabled with the partition key {@code timestamp_field} and a {@code DAY} time unit,
     * when
     * {@link PartitioningStrategyFactory#createPartitioningStrategy(ProtobufConverterOrchestrator, MaxComputeSinkConfig, Descriptors.Descriptor)}
     * is invoked, then the returned strategy is asserted to be a {@link TimestampPartitioningStrategy}.</p>
     */
    @Test
    public void shouldReturnTimestampPartitionStrategy() {
        String timestampFieldName = "timestamp_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(timestampFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(timestampFieldName);
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);


        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        assertTrue(partitioningStrategy instanceof TimestampPartitioningStrategy);
    }

    /**
     * Verifies that no strategy is created when partitioning is disabled.
     *
     * <p>Given a {@link MaxComputeSinkConfig} whose {@link MaxComputeSinkConfig#isTablePartitioningEnabled()}
     * returns {@code false}, when
     * {@link PartitioningStrategyFactory#createPartitioningStrategy(ProtobufConverterOrchestrator, MaxComputeSinkConfig, Descriptors.Descriptor)}
     * is invoked, then it returns {@code null}.</p>
     */
    @Test
    public void shouldReturnNull() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);


        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        Assert.assertNull(partitioningStrategy);
    }

    /**
     * Verifies that an unsupported partition key type is rejected.
     *
     * <p>Given partitioning enabled with the partition key {@code float_field}, whose resolved MaxCompute type
     * is not an allowed partition type, when
     * {@link PartitioningStrategyFactory#createPartitioningStrategy(ProtobufConverterOrchestrator, MaxComputeSinkConfig, Descriptors.Descriptor)}
     * is invoked, then an {@link IllegalArgumentException} is thrown, as asserted by the {@code expected}
     * attribute of the {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenTypeInfoIsNotSupported() {
        String unsupportedTypeFieldName = "float_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(unsupportedTypeFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(unsupportedTypeFieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);


        PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
    }

    /**
     * Verifies that a partition key absent from the descriptor is rejected.
     *
     * <p>Given partitioning enabled with the partition key {@code non_existent_field}, which the descriptor does
     * not declare, when
     * {@link PartitioningStrategyFactory#createPartitioningStrategy(ProtobufConverterOrchestrator, MaxComputeSinkConfig, Descriptors.Descriptor)}
     * is invoked, then an {@link IllegalArgumentException} is thrown, as asserted by the {@code expected}
     * attribute of the {@link Test} annotation.</p>
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenFieldIsNotFoundInDescriptor() {
        String fieldName = "non_existent_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(fieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(fieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getMaxNestedMessageDepth()).thenReturn(15);


        PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
    }
}
