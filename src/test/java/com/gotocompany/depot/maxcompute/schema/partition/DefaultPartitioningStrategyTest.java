package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DefaultPartitioningStrategy}.
 *
 * <p>These tests verify the value-based partitioning strategy used for non-timestamp partition keys. A
 * {@link MaxComputeSinkConfig} mock supplies the partition key name {@code tablePartitionKey} and the partition
 * column name {@code tablePartitionColumnName}, and the strategy is constructed with a {@code STRING} type. The
 * tests assert the reported original column name, the generated partition {@link Column}, the
 * replace-original-column flag, and the {@link com.aliyun.odps.PartitionSpec} rendering for both a present value
 * and a {@code null} value.</p>
 *
 * @see DefaultPartitioningStrategy
 */
public class DefaultPartitioningStrategyTest {
    /**
     * Verifies that the original partition column name is the configured table partition key.
     *
     * <p>Given a {@link DefaultPartitioningStrategy} built from the shared config mock, when
     * {@link DefaultPartitioningStrategy#getOriginalPartitionColumnName()} is called, then it returns
     * {@code tablePartitionKey}.</p>
     */
    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertEquals("tablePartitionKey", defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    /**
     * Verifies that the partition column is built from the configured column name and the strategy's type.
     *
     * <p>Given a strategy constructed with a {@code STRING} type, when
     * {@link DefaultPartitioningStrategy#getPartitionColumn()} is called, then the returned {@link Column} equals
     * a {@code STRING} column named by {@link MaxComputeSinkConfig#getTablePartitionColumnName()}.</p>
     */
    @Test
    public void shouldReturnPartitionColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                maxComputeSinkConfig);
        Column expectedColumn = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        assertEquals(expectedColumn, defaultPartitioningStrategy.getPartitionColumn());
    }

    /**
     * Verifies that value-based partitioning replaces the original column.
     *
     * <p>Given a {@link DefaultPartitioningStrategy}, when
     * {@link DefaultPartitioningStrategy#shouldReplaceOriginalColumn()} is called, then it returns {@code true},
     * indicating the partition column reuses the original field rather than being added alongside it.</p>
     */
    @Test
    public void shouldReturnTrueForReplacingOriginalColumn() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertTrue(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    /**
     * Verifies that a non-null partition value renders as {@code columnName='value'}.
     *
     * <p>Given the partition value {@code "object"}, when
     * {@link DefaultPartitioningStrategy#getPartitionSpec(Object)} is called, then the resulting partition
     * specification's string form equals {@code tablePartitionColumnName='object'}.</p>
     */
    @Test
    public void shouldReturnValidPartitionSpec() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());
        String partitionKey = "object";
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='object'";

        assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(partitionKey)
                .toString());
    }

    /**
     * Verifies that a {@code null} partition value falls back to the {@code __NULL__} sentinel.
     *
     * <p>Given a {@code null} partition value, when
     * {@link DefaultPartitioningStrategy#getPartitionSpec(Object)} is called, then the resulting partition
     * specification's string form equals {@code tablePartitionColumnName='__NULL__'}.</p>
     */
    @Test
    public void shouldReturnDefaultPartitionSpec() {
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='__NULL__'";
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(null)
                .toString());
    }

    /**
     * Builds the shared {@link MaxComputeSinkConfig} mock used across the tests.
     *
     * <p>Stubs {@link MaxComputeSinkConfig#getTablePartitionColumnName()} to return
     * {@code tablePartitionColumnName} and {@link MaxComputeSinkConfig#getTablePartitionKey()} to return
     * {@code tablePartitionKey}.</p>
     *
     * @return a configured {@link MaxComputeSinkConfig} mock
     */
    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("tablePartitionKey");
        return maxComputeSinkConfig;
    }
}
