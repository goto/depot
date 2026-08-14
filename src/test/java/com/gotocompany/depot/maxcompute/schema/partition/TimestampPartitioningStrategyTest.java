package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link TimestampPartitioningStrategy}.
 *
 * <p>These tests verify the timestamp-based partitioning strategy, which retains the original timestamp field
 * and adds a generated {@code STRING} partition column populated by truncating the timestamp to a configured
 * time unit. A {@link MaxComputeSinkConfig} mock supplies the partition key {@code event_timestamp}, the
 * partition column name {@code tablePartitionColumnName}, and the {@code DAY} time unit. Where a partition spec
 * is computed, a {@link TableSchema} carrying the source column and the generated partition column (with its
 * {@link TruncTime} expression) is built and a populated {@link ArrayRecord} is supplied.</p>
 *
 * @see TimestampPartitioningStrategy
 */
public class TimestampPartitioningStrategyTest {

    /**
     * Verifies that the original partition column name is the configured timestamp partition key.
     *
     * <p>Given a {@link TimestampPartitioningStrategy} built from the shared config mock, when
     * {@link TimestampPartitioningStrategy#getOriginalPartitionColumnName()} is called, then it returns
     * {@code event_timestamp}.</p>
     */
    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        assertEquals("event_timestamp",
                defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    /**
     * Verifies that timestamp partitioning does not replace the original column.
     *
     * <p>Given a {@link TimestampPartitioningStrategy}, when
     * {@link TimestampPartitioningStrategy#shouldReplaceOriginalColumn()} is called, then it returns
     * {@code false}, indicating the generated partition column is added alongside the original timestamp
     * field.</p>
     */
    @Test
    public void shouldReturnFalseForReplacingOriginalColumn() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        assertFalse(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    /**
     * Verifies that the generated partition column is a {@code STRING} column named by configuration.
     *
     * <p>Given a {@link TimestampPartitioningStrategy}, when
     * {@link TimestampPartitioningStrategy#getPartitionColumn()} is called, then the returned {@link Column}
     * equals a {@code STRING} column named {@code tablePartitionColumnName}.</p>
     */
    @Test
    public void shouldReturnValidColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);

        Column column = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        assertEquals(column, timestampPartitioningStrategy.getPartitionColumn());
    }

    /**
     * Verifies that the partition spec is derived by truncating the record's timestamp to the day.
     *
     * <p>Given a record whose {@code event_timestamp} is the UTC instant for epoch second {@code 1730134810}
     * and a schema whose partition column truncates that field by {@code DAY}, when
     * {@link TimestampPartitioningStrategy#getPartitionSpec(Object)} is called, then the resulting partition
     * specification's string form equals {@code tablePartitionColumnName='2024-10-28'}.</p>
     */
    @Test
    public void shouldReturnValidPartitionSpec() {
        //October 29, 2024 12:00:10 AM GMT+07:00
        long epoch = 1730134810;
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);
        Column partitionColumn = Column.newBuilder("tablePartitionColumnName", TypeInfoFactory.STRING)
                .build();
        partitionColumn.setGenerateExpression(new TruncTime("event_timestamp", "DAY"));
        TableSchema tableSchema = TableSchema.builder()
                .withStringColumn("str")
                .withColumn(Column.newBuilder("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ)
                        .build())
                .withPartitionColumn(partitionColumn)
                .build();
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = Mockito.mock(MaxComputeSchema.class);
        when(maxComputeSchema.getTableSchema()).thenReturn(tableSchema);
        when(maxComputeSchemaCache.getMaxComputeSchema())
                .thenReturn(maxComputeSchema);
        String expectedStartOfDayEpoch = "2024-10-28";
        Record record = new ArrayRecord(tableSchema);
        record.set("str", "strVal");
        record.set("event_timestamp", LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC));

        assertEquals(String.format("tablePartitionColumnName='%s'", expectedStartOfDayEpoch),
                timestampPartitioningStrategy.getPartitionSpec(record).toString());
    }

    /**
     * Verifies that a non-record argument produces an empty partition spec.
     *
     * <p>Given a {@link String} argument rather than a MaxCompute record, when
     * {@link TimestampPartitioningStrategy#getPartitionSpec(Object)} is called, then an empty partition
     * specification is returned, whose string form is the empty string.</p>
     */
    @Test
    public void shouldEmptyPartitionSpecIfObjectIsNotRecord() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);

        assertEquals("",
                timestampPartitioningStrategy.getPartitionSpec("").toString());
    }

    /**
     * Verifies that a record with a null timestamp produces the {@code __NULL__} partition value.
     *
     * <p>Given a record whose {@code event_timestamp} is {@code null} and a schema whose partition column
     * truncates that field by {@code DAY}, when
     * {@link TimestampPartitioningStrategy#getPartitionSpec(Object)} is called, then the resulting partition
     * specification's string form equals {@code tablePartitionColumnName='__NULL__'}.</p>
     */
    @Test
    public void shouldReturnDefaultPartitionSpec() {
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='__NULL__'";
        TimestampPartitioningStrategy timestampPartitioningStrategy = new TimestampPartitioningStrategy(getMaxComputeSinkConfig());
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = Mockito.mock(MaxComputeSchema.class);
        Column partitionColumn = Column.newBuilder("tablePartitionColumnName", TypeInfoFactory.STRING)
                .build();
        partitionColumn.setGenerateExpression(new TruncTime("event_timestamp", "DAY"));
        TableSchema tableSchema = TableSchema.builder()
                .withStringColumn("str")
                .withDatetimeColumn("event_timestamp")
                .withPartitionColumn(partitionColumn)
                .build();
        when(maxComputeSchema.getTableSchema()).thenReturn(tableSchema);
        when(maxComputeSchemaCache.getMaxComputeSchema())
                .thenReturn(maxComputeSchema);
        Record record = new ArrayRecord(tableSchema);
        record.set("str", "strVal");
        record.set("event_timestamp", null);

        assertEquals(expectedPartitionSpecStringRepresentation,
                timestampPartitioningStrategy.getPartitionSpec(record)
                        .toString());
    }

    /**
     * Builds the shared {@link MaxComputeSinkConfig} mock used across the tests.
     *
     * <p>Stubs partitioning as enabled with the partition column name {@code tablePartitionColumnName}, the
     * partition key {@code event_timestamp}, and the {@code DAY} timestamp truncation time unit.</p>
     *
     * @return a configured {@link MaxComputeSinkConfig} mock
     */
    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled())
                .thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit())
                .thenReturn("DAY");
        return maxComputeSinkConfig;
    }

}
