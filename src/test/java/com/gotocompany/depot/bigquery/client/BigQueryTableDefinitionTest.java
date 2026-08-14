package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.bigquery.exception.BQClusteringKeysException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BigQueryTableDefinition}, which derives a {@link StandardTableDefinition}
 * (schema, time-partitioning and clustering) from a {@link BigQuerySinkConfig}.
 *
 * <p>Each test stubs the relevant partitioning and clustering options on a mocked
 * {@link BigQuerySinkConfig}, supplies a BigQuery {@link Schema} and asserts on the resulting
 * partitioning/clustering configuration. Invalid combinations are expected to raise a
 * {@link NullPointerException}, {@link UnsupportedOperationException}, {@link RuntimeException} or
 * {@link BQClusteringKeysException}, the last validated through the {@link ExpectedException}
 * rule.</p>
 */
public class BigQueryTableDefinitionTest {

    /** JUnit rule used to assert the type and message of expected {@link BQClusteringKeysException}s. */
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();
    /** Mocked sink configuration whose partitioning and clustering options each test stubs. */
    @Mock
    private BigQuerySinkConfig bqConfig;

    /**
     * Initializes a fresh mock {@link BigQuerySinkConfig} before each test.
     */
    @Before
    public void setup() {
        bqConfig = Mockito.mock(BigQuerySinkConfig.class);
    }

    /**
     * Verifies that a {@code null} schema is rejected when partitioning and clustering are enabled.
     *
     * <p>Given partitioning and clustering enabled with their keys configured, when
     * {@code getTableDefinition(null)} is called, then a {@link NullPointerException} is thrown.</p>
     */
    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionWhenSchemaIsNull() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(null);
    }

    /**
     * Verifies that partitioning on a non-timestamp column is unsupported.
     *
     * <p>Given partitioning enabled with the partition key pointing at an integer field, when the
     * table definition is built, then an {@link UnsupportedOperationException} is thrown because range
     * partitioning is not supported.</p>
     */
    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionForRangePartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("int_field");

        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    /**
     * Verifies that enabling partitioning without a partition key fails.
     *
     * <p>Given partitioning enabled but no partition key configured, when the table definition is
     * built from a schema that lacks the (null) partition field, then a {@link RuntimeException} is
     * thrown.</p>
     */
    @Test(expected = RuntimeException.class)
    public void shouldThrowErrorIfPartitionFieldNotSet() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    /**
     * Verifies that the configured partition expiry is applied to time-partitioning.
     *
     * <p>Given partitioning enabled on a {@code timestamp_field} with a partition expiry, when the
     * table definition is built, then its {@link TimePartitioning} targets that field and carries the
     * configured expiration in milliseconds.</p>
     */
    @Test
    public void shouldReturnTimePartitioningWithPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
        assertEquals(partitionExpiry, tableDefinition.getTimePartitioning().getExpirationMs().longValue());
    }

    /**
     * Verifies clustering on a single column.
     *
     * <p>Given clustering enabled with one clustering key present in the schema, when the table
     * definition is built, then its clustering fields contain exactly that column.</p>
     */
    @Test
    public void shouldReturnClusteringWithSingleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        List<String> expectedColumns = Collections.singletonList("string_field");
        assertEquals(expectedColumns, tableDefinition.getClustering().getFields());
    }

    /**
     * Verifies clustering on the maximum of four columns.
     *
     * <p>Given clustering enabled with four clustering keys all present in the schema, when the table
     * definition is built, then its clustering fields equal the four configured columns in order.</p>
     */
    @Test
    public void shouldReturnClusteringWithMultipleColumns() {
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        List<String> expectedColumns = Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field");
        assertEquals(expectedColumns, tableDefinition.getClustering().getFields());
    }

    /**
     * Verifies that enabling clustering without any clustering keys fails.
     *
     * <p>Given clustering enabled but no clustering keys configured (and a named table), when the
     * table definition is built, then a {@link BQClusteringKeysException} is raised whose message
     * states that the clustering key is not specified for {@code table_name}.</p>
     */
    @Test
    public void shouldThrowExceptionIfClusteringKeyIsNotSet() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Clustering key not specified for the table: table_name");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("table_name");

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    /**
     * Verifies that more than four clustering keys are rejected.
     *
     * <p>Given clustering enabled with five clustering keys, when the table definition is built, then
     * a {@link BQClusteringKeysException} is raised stating that the maximum number of clustering
     * columns is four.</p>
     */
    @Test
    public void shouldThrowExceptionIfClusteringKeyIsSetMoreThanFour() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("Max number of columns for clustering is 4");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Arrays.asList("string_field", "int_field", "bool_field", "timestamp_field", "another_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field", LegacySQLTypeName.STRING),
                Field.of("int_field", LegacySQLTypeName.INTEGER),
                Field.of("bool_field", LegacySQLTypeName.BOOLEAN),
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("another_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    /**
     * Verifies that clustering keys absent from the schema are rejected.
     *
     * <p>Given clustering enabled with a key that does not exist in the supplied schema, when the
     * table definition is built, then a {@link BQClusteringKeysException} is raised reporting the
     * missing column.</p>
     */
    @Test
    public void shouldThrowExceptionIfClusteringKeyNotExistInSchema() {
        expectedEx.expect(BQClusteringKeysException.class);
        expectedEx.expectMessage("One or more column names specified [string_field] not exist on the schema or a nested type which is not supported for clustering");

        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("string_field2", LegacySQLTypeName.STRING),
                Field.of("int_field2", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        bigQueryTableDefinition.getTableDefinition(bqSchema);
    }

    /**
     * Verifies a table definition that is both partitioned and clustered.
     *
     * <p>Given partitioning on a timestamp field and clustering on a string field, when the table
     * definition is built, then the returned schema matches the input and both the time-partitioning
     * field and the clustering fields are set accordingly.</p>
     */
    @Test
    public void shouldReturnPartitionedAndClusteredTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("timestamp_field")
                .build();

        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNotNull(returnedPartitioning);
        assertEquals(returnedPartitioning.getField(), partitioning.getField());
        assertNotNull(returnedClustering);
        assertEquals(returnedClustering.getFields(), clustering.getFields());
    }

    /**
     * Verifies a partitioned table definition with clustering disabled.
     *
     * <p>Given partitioning enabled and clustering disabled, when the table definition is built, then
     * the schema matches the input, time-partitioning targets the configured field and the clustering
     * is {@code null}.</p>
     */
    @Test
    public void shouldReturnPartitionedTableWithoutClusteredTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        TimePartitioning partitioning = TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
                .setField("timestamp_field")
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNotNull(returnedPartitioning);
        assertEquals(returnedPartitioning.getField(), partitioning.getField());
        assertNull(returnedClustering);
    }

    /**
     * Verifies a clustered table definition with partitioning disabled.
     *
     * <p>Given clustering enabled and partitioning disabled, when the table definition is built, then
     * the schema matches the input, clustering targets the configured field and the time-partitioning
     * is {@code null}.</p>
     */
    @Test
    public void shouldReturnClusteredTableWithoutPartitionTableDefinition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(true);
        when(bqConfig.getTableClusteringKeys()).thenReturn(Collections.singletonList("string_field"));

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        Clustering clustering = Clustering.newBuilder()
                .setFields(Collections.singletonList("string_field"))
                .build();

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNull(returnedPartitioning);
        assertNotNull(returnedClustering);
        assertEquals(returnedClustering.getFields(), clustering.getFields());
    }

    /**
     * Verifies a plain table definition with neither partitioning nor clustering.
     *
     * <p>Given both partitioning and clustering disabled, when the table definition is built, then the
     * schema matches the input and both the time-partitioning and clustering are {@code null}.</p>
     */
    @Test
    public void shouldReturnTableDefinitionWithoutPartitioningAndClustering() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.isTableClusteringEnabled()).thenReturn(false);

        Schema bqSchema = Schema.of(
                Field.of("timestamp_field", LegacySQLTypeName.TIMESTAMP),
                Field.of("string_field", LegacySQLTypeName.STRING)
        );

        BigQueryTableDefinition bigQueryTableDefinition = new BigQueryTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bigQueryTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        TimePartitioning returnedPartitioning = tableDefinition.getTimePartitioning();
        Clustering returnedClustering = tableDefinition.getClustering();

        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals(returnedSchema.getFields().get(1).getName(), bqSchema.getFields().get(1).getName());
        assertEquals(returnedSchema.getFields().get(1).getMode(), bqSchema.getFields().get(1).getMode());
        assertEquals(returnedSchema.getFields().get(1).getType(), bqSchema.getFields().get(1).getType());
        assertNull(returnedPartitioning);
        assertNull(returnedClustering);
    }
}
