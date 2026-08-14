package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.*;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link BigQueryClient}, which creates and updates the BigQuery dataset and table so
 * that they match the configured schema, partitioning and clustering.
 *
 * <p>Each test stubs a mocked {@link BigQuery} service together with a mocked
 * {@link BigQuerySinkConfig} and the BigQuery {@link Dataset}/{@link Table} handles, then invokes
 * {@code upsertTable} with a set of {@link Field}s. Assertions verify whether the client creates the
 * dataset and table, updates an existing table, retries transient update failures, leaves an
 * unchanged table alone, or fails when the table update errors or the dataset location has changed.
 * The {@link MockitoJUnitRunner} initializes the {@link Mock}-annotated collaborators.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class BigQueryClientTest {

    /** Mocked BigQuery service used to create, read and update datasets and tables. */
    @Mock
    private BigQuery bigquery;
    /** Mocked sink configuration supplying table, dataset, partitioning and clustering settings. */
    @Mock
    private BigQuerySinkConfig bqConfig;
    /** Mocked dataset handle whose existence, location and labels are stubbed per test. */
    @Mock
    private Dataset dataset;
    /** Mocked table handle whose existence and definition are stubbed per test. */
    @Mock
    private Table table;
    /** Mocked table definition used when stubbing an existing table's schema and partitioning. */
    @Mock
    private StandardTableDefinition mockTableDefinition;
    /** Mocked time-partitioning used when stubbing an existing table's partitioning. */
    @Mock
    private TimePartitioning mockTimePartitioning;
    /** Mocked clustering used when stubbing an existing table's clustering columns. */
    @Mock
    private Clustering mockClustering;
    /** Mocked instrumentation used to verify informational logging. */
    @Mock
    private Instrumentation instrumentation;
    /** Mocked metrics collaborator. */
    @Mock
    private BigQueryMetrics metrics;

    /** Client under test, constructed within each test once its configuration has been stubbed. */
    private BigQueryClient bqClient;

    /**
     * Verifies that a non-existent dataset and table are created on upsert.
     *
     * <p>Given partitioning disabled and both the dataset and table reported as not existing, when
     * {@code upsertTable} runs with the schema fields, then the client creates the dataset in the
     * {@code US} location and creates the table, and never updates it.</p>
     */
    @Test
    public void shouldIgnoreExceptionIfDatasetAlreadyExists() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(false);
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(DatasetInfo.newBuilder(tableId.getDataset()).setLocation("US").build());
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies that transient table-update failures are retried until success.
     *
     * <p>Given an existing dataset and table whose definition differs from the desired schema, and an
     * update that throws a rate-limit {@link BigQueryException} three times before succeeding, when
     * {@code upsertTable} runs, then the table is never created and {@code update} is invoked four
     * times in total.</p>
     */
    @Test
    public void shouldUpsertWithRetries() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getDatasetLabels()).thenReturn(Collections.emptyMap());
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLabels()).thenReturn(new HashMap<String, String>() {{
            put("new_key", "new_value");
        }});
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenThrow(new BigQueryException(500, " Error while updating bigquery table on callback:Exceeded rate limits: too many table update operations"))
                .thenReturn(table);
        bqClient.upsertTable(updatedBQSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, times(4)).update(tableInfo);
    }

    /**
     * Verifies that an unchanged table is neither created nor updated.
     *
     * <p>Given an existing dataset and a table whose definition already matches the desired schema,
     * when {@code upsertTable} runs, then the client neither creates nor updates the table.</p>
     */
    @Test
    public void shouldNotUpdateTableIfTableAlreadyExistsWithSameSchema() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(table.exists()).thenReturn(true);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies that an existing table is updated when the schema changes.
     *
     * <p>Given an existing dataset and table and a desired schema with an additional field, when
     * {@code upsertTable} runs, then the client updates the table once and never creates it.</p>
     */
    @Test
    public void shouldUpdateTableIfTableAlreadyExistsAndSchemaChanges() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo)).thenReturn(table);

        bqClient.upsertTable(updatedBQSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    /**
     * Verifies that an existing partitioned table is updated to apply a partition expiry.
     *
     * <p>Given partitioning enabled with an expiry and an existing table whose time-partitioning has
     * no expiration set, when {@code upsertTable} runs, then the client updates the table once and
     * never creates it.</p>
     */
    @Test
    public void shouldUpdateTableIfTableNeedsToSetPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTimePartitioning.getExpirationMs()).thenReturn(null);
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    /**
     * Verifies that a non-retryable update failure propagates to the caller.
     *
     * <p>Given an existing table whose update throws a {@code 404} {@link BigQueryException}, when
     * {@code upsertTable} runs with a changed schema, then the exception is propagated.</p>
     */
    @Test(expected = BigQueryException.class)
    public void shouldThrowExceptionIfUpdateTableFails() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        BigQueryTableDefinition bqDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition standardTableDefinition = bqDefinition.getTableDefinition(bqSchema);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        StandardTableDefinition updatedBigQueryTableDefinition = bqDefinition.getTableDefinition(Schema.of(updatedBQSchemaFields));

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBigQueryTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(standardTableDefinition);
        when(bigquery.update(tableInfo)).thenThrow(new BigQueryException(404, "Failed to update"));

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(updatedBQSchemaFields);
    }

    /**
     * Verifies that changing the dataset location is rejected.
     *
     * <p>Given a configured dataset location that differs from the existing dataset's actual location,
     * when {@code upsertTable} runs, then a {@link RuntimeException} is thrown and neither create nor
     * update is performed.</p>
     */
    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("new-location");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies creation of a new table with both partitioning and clustering.
     *
     * <p>Given partitioning and clustering enabled and a non-existent table, when {@code upsertTable}
     * runs, then the client creates the table once and never updates it.</p>
     */
    @Test
    public void shouldCreateBigQueryTableWithPartitionAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("string_field", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};
        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies creation of a new partitioned (non-clustered) table.
     *
     * <p>Given partitioning enabled and a non-existent table, when {@code upsertTable} runs, then the
     * client creates the table once and never updates it.</p>
     */
    @Test
    public void shouldCreateBigQueryTableWithPartitionOnly() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies creation of a new clustered (non-partitioned) table.
     *
     * <p>Given clustering enabled, partitioning disabled and a non-existent table, when
     * {@code upsertTable} runs, then the client creates the table once and never updates it.</p>
     */
    @Test
    public void shouldCreateBigQueryTableWithClusteringOnly() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("string_field", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);
        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies creation of a new plain table with neither partitioning nor clustering.
     *
     * <p>Given partitioning and clustering disabled and a non-existent table, when {@code upsertTable}
     * runs, then the client creates the table once and never updates it.</p>
     */
    @Test
    public void shouldCreateBigQueryTableWithoutPartitionAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("load_time", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(false);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    /**
     * Verifies that clustering columns on an existing clustered table are modified when they differ.
     *
     * <p>Given an existing table clustered on {@code [id]} but configured to cluster on
     * {@code [id, city]}, when {@code upsertTable} runs, then the client updates the table once and
     * never creates it.</p>
     */
    @Test
    public void shouldModifyClusteringColumnsFromExistingClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(mockClustering);
        when(mockClustering.getFields()).thenReturn(Collections.singletonList("id"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    /**
     * Verifies that clustering is added to an existing unpartitioned, unclustered table.
     *
     * <p>Given an existing table with neither partitioning nor clustering and a configuration enabling
     * clustering on {@code [id, city]}, when {@code upsertTable} runs, then the client updates the
     * table once and never creates it.</p>
     */
    @Test
    public void shouldAddClusteringColumnsFromExistingUnPartitionedAndUnClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(null);
        when(mockTableDefinition.getTimePartitioning()).thenReturn(null);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    /**
     * Verifies that clustering is added to an existing partitioned but unclustered table.
     *
     * <p>Given an existing partitioned table without clustering and a configuration enabling both
     * partitioning and clustering on {@code [id, city]}, when {@code upsertTable} runs, then the client
     * updates the table once and never creates it.</p>
     */
    @Test
    public void shouldAddClusteringColumnsFromExistingPartitionedAndUnClusteredTable() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("event_timestamp");
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTableDefinition.getClustering()).thenReturn(null);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    /**
     * Verifies that an unchanged clustering configuration produces no table update.
     *
     * <p>Given an existing table already clustered on {@code [id, city]} matching the configuration,
     * when {@code upsertTable} runs, then the client neither creates nor updates the table and logs
     * that the update is skipped because the proto schema is unchanged.</p>
     */
    @Test
    public void shouldNotModifyClusteringColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("id", "city"));
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, metrics, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("city", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        Schema bqSchema = Schema.of(bqSchemaFields);
        StandardTableDefinition standardTableDefinition = new BigQueryTableDefinition(bqConfig).getTableDefinition(bqSchema);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, standardTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(true);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(standardTableDefinition.getType());
        when(mockTableDefinition.getSchema()).thenReturn(standardTableDefinition.getSchema());
        when(mockTableDefinition.getClustering()).thenReturn(mockClustering);
        when(mockClustering.getFields()).thenReturn(Arrays.asList("id", "city"));

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
        verify(instrumentation, times(1)).logInfo("Skipping bigquery table update, since proto schema hasn't changed");
    }
}
