package com.gotocompany.depot.bigquery.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverter;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link BigqueryJsonUpdateListener}, which prepares the BigQuery table and converter
 * for JSON sinks when the dynamic schema is (re)initialized.
 *
 * <p>The listener is built from a {@link BigQuerySinkConfig} (created via {@code ConfigFactory}), a
 * mocked {@link MessageRecordConverterCache}, a mocked {@link BigQueryClient} (whose current schema is
 * stubbed) and a mocked {@link Instrumentation}. The tests assert that {@code updateSchema} registers
 * a {@link MessageRecordConverter} and upserts the table with the default, metadata and existing
 * columns (honouring partition-key typing), and that invalid configurations such as colliding
 * columns, a metadata namespace, a wrong partition-key type or a disabled dynamic schema raise the
 * expected exceptions.</p>
 */
public class BigqueryJsonUpdateListenerTest {

    /** Mocked converter cache whose converter registration is verified. */
    private MessageRecordConverterCache converterCache;
    /** Mocked BigQuery client whose current schema is stubbed and whose upserts are verified. */
    private BigQueryClient mockBqClient;
    /** Mocked instrumentation collaborator. */
    private Instrumentation instrumentation;

    /**
     * Initializes the mocks and stubs an empty current schema before each test.
     *
     * @throws Exception if mock setup fails
     */
    @Before
    public void setUp() throws Exception {
        converterCache = mock(MessageRecordConverterCache.class);
        mockBqClient = mock(BigQueryClient.class);
        Schema emptySchema = Schema.of();
        when(mockBqClient.getSchema()).thenReturn(emptySchema);
        instrumentation = mock(Instrumentation.class);
    }

    /**
     * Verifies that updating the schema registers a converter and upserts the table.
     *
     * <p>Given a default configuration, when {@code updateSchema} runs, then a
     * {@link MessageRecordConverter} is registered in the cache and the client upserts the table with
     * an empty field list.</p>
     */
    @Test
    public void shouldSetMessageRecordConverterAndUpsertTable() {
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, Collections.emptyMap());
        BigqueryJsonUpdateListener updateListener = new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, mockBqClient, instrumentation);
        updateListener.setMessageParser(null);
        updateListener.updateSchema();
        verify(converterCache, times(1)).setMessageRecordConverter(any(MessageRecordConverter.class));
        verify(mockBqClient, times(1)).upsertTable(Collections.emptyList());
    }

    /**
     * Verifies that configured default columns are created on the table.
     *
     * <p>Given default columns {@code event_timestamp} (timestamp) and {@code first_name} (string),
     * when {@code updateSchema} runs, then the client upserts the table with exactly those two nullable
     * fields.</p>
     */
    @Test
    public void shouldCreateTableWithDefaultColumns() {

        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_CONNECTOR_DEFAULT_DATATYPE_STRING_ENABLE", "false"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

    /**
     * Verifies that default columns and enabled metadata columns are created together.
     *
     * <p>Given default columns plus metadata column types with metadata enabled, when
     * {@code updateSchema} runs, then the client upserts the table with the default and metadata fields
     * (asserted irrespective of order).</p>
     */
    @Test
    public void shouldCreateTableWithDefaultColumnsAndMetadataFields() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build()
        );
        ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
        assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
    }


    /**
     * Verifies that default-column types are respected even with string casting enabled.
     *
     * <p>Given a {@code first_name} default column typed as integer (with string casting enabled) plus
     * metadata columns, when {@code updateSchema} runs, then the client upserts the table with the
     * declared default types and the metadata fields.</p>
     */
    @Test
    public void shouldCreateTableWithDefaultColumnsWithDdifferentTypesAndMetadataFields() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=integer",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true",
                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("first_name", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_offset", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_topic", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("message_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
        ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
        assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
    }

    /**
     * Verifies that metadata columns are omitted when metadata is disabled.
     *
     * <p>Given default columns and metadata column types but metadata disabled, when
     * {@code updateSchema} runs, then the client upserts the table with only the default columns.</p>
     */
    @Test
    public void shouldNotAddMetadataFields() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,message_topic=string,message_timestamp=timestamp",
                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "false"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
        assertThat(listArgumentCaptor.getValue(), containsInAnyOrder(bqSchemaFields.toArray()));
    }

    /**
     * Verifies that a column shared by default columns and metadata is rejected.
     *
     * <p>Given {@code first_name} present in both the default columns and the metadata column types
     * with metadata enabled, when {@code updateSchema} runs, then an {@link IllegalArgumentException}
     * is thrown.</p>
     */
    @Test
    public void shouldThrowErrorIfDefaultColumnsAndMetadataFieldsContainSameEntryCalledFirstName() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false",
                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,first_name=integer",
                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        assertThrows(IllegalArgumentException.class, bigqueryJsonUpdateListener::updateSchema);
    }

    /**
     * Verifies that configuring a metadata namespace is unsupported for JSON sinks.
     *
     * <p>Given metadata enabled with a non-empty metadata namespace, when {@code updateSchema} runs,
     * then an {@link UnsupportedOperationException} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorIfMetadataNamespaceIsNotEmpty() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,first_name=integer",
                "SINK_BIGQUERY_ADD_METADATA_ENABLED", "true",
                "SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_namespace"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        assertThrows(UnsupportedOperationException.class, bigqueryJsonUpdateListener::updateSchema);
    }

    /**
     * Verifies that existing table columns are preserved alongside default columns.
     *
     * <p>Given an existing schema with two fields and configured default columns, when
     * {@code updateSchema} runs, then the client upserts the table with both the default columns and
     * the pre-existing fields.</p>
     */
    @Test
    public void shouldCreateTableWithDefaultColumnsAndExistingTableColumns() {
        Field existingField1 = Field.of("existing_field1", LegacySQLTypeName.STRING);
        Field existingField2 = Field.of("existing_field2", LegacySQLTypeName.STRING);
        when(mockBqClient.getSchema()).thenReturn(Schema.of(existingField1,
                existingField2));
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        ArgumentCaptor<List<Field>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(mockBqClient, times(1)).upsertTable(listArgumentCaptor.capture());
        List<Field> actualFields = listArgumentCaptor.getValue();
        Field eventTimestampField = Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build();
        Field firstNameField = Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        assertThat(actualFields, containsInAnyOrder(eventTimestampField, firstNameField, existingField1, existingField2));
    }

    /**
     * Verifies that the partition key retains its declared type rather than being cast to string.
     *
     * <p>Given partitioning enabled on {@code event_timestamp} (a timestamp default column) with string
     * casting enabled, when {@code updateSchema} runs, then the client upserts the table with
     * {@code event_timestamp} kept as a timestamp and {@code first_name} as a string.</p>
     */
    @Test
    public void shouldNotCastPartitionKeyToString() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=timestamp,first_name=string",
                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp",
                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        bigqueryJsonUpdateListener.updateSchema();
        List<Field> bqSchemaFields = ImmutableList.of(
                Field.newBuilder("event_timestamp", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("first_name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
        verify(mockBqClient, times(1)).upsertTable(bqSchemaFields);
    }

    /**
     * Verifies that a partition key declared with a non-timestamp type is rejected.
     *
     * <p>Given partitioning enabled on {@code event_timestamp} declared as an integer default column,
     * when {@code updateSchema} runs, then an {@link UnsupportedOperationException} is thrown.</p>
     */
    @Test
    public void shouldThrowErrorWhenPartitionKeyTypeIsNotCorrect() {
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp=integer,first_name=string",
                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp",
                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "true"
        ));
        BigqueryJsonUpdateListener bigqueryJsonUpdateListener = new BigqueryJsonUpdateListener(config, converterCache, mockBqClient, instrumentation);
        assertThrows(UnsupportedOperationException.class, bigqueryJsonUpdateListener::updateSchema);
    }

    /**
     * Verifies that constructing the listener requires dynamic schema to be enabled.
     *
     * <p>Given a configuration with dynamic schema disabled, when a
     * {@link BigqueryJsonUpdateListener} is constructed, then an {@link UnsupportedOperationException}
     * is thrown.</p>
     */
    @Test
    public void shouldThrowExceptionWhenDynamicSchemaNotEnabled() {
        BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class,
                ImmutableMap.of("SINK_BIGQUERY_DYNAMIC_SCHEMA_ENABLE", "false"));
        assertThrows(UnsupportedOperationException.class,
                () -> new BigqueryJsonUpdateListener(bigQuerySinkConfig, converterCache, mockBqClient, instrumentation));

    }
}
