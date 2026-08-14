package com.gotocompany.depot.bigquery.handler;

import com.google.api.client.util.DateTime;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.models.Record;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link JsonErrorHandler}, which reacts to BigQuery insert errors for JSON sinks by
 * adding the missing columns to the table schema.
 *
 * <p>The tests drive the handler with a mocked {@link BigQueryClient} (whose current schema is
 * stubbed) and a {@link BigQuerySinkConfig} built via {@code ConfigFactory}. They supply a map of
 * per-row {@link BigQueryError}s together with the corresponding {@link Record}s, then capture the
 * field list passed to {@code upsertTable} to assert which missing, metadata, default and partition
 * columns are added (and that unrelated errors are ignored or unsupported configurations rejected).
 * Mockito annotations are initialized in {@link #setUp()}, with the captured fields exposed through
 * {@code fieldsArgumentCaptor}.</p>
 */
public class JsonErrorHandlerTest {

    /** Empty BigQuery schema returned as the table's current schema by default. */
    private final Schema emptyTableSchema = Schema.of();

    /** Default sink configuration created from an empty property map. */
    private final BigQuerySinkConfig bigQuerySinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, Collections.emptyMap());

    /** Mocked BigQuery client whose schema is stubbed and whose table upserts are verified. */
    @Mock
    private BigQueryClient bigQueryClient;

    /** Captures the list of {@link Field}s passed to {@code upsertTable}. */
    @Captor
    private ArgumentCaptor<List<Field>> fieldsArgumentCaptor;

    /** Mocked instrumentation collaborator. */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Initializes the Mockito-annotated mocks before each test.
     *
     * @throws Exception if mock initialization fails
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Builds a nullable BigQuery {@link Field} of the given name and type.
     *
     * @param name the field name
     * @param type the BigQuery field type
     * @return a {@code NULLABLE} {@link Field} with the supplied name and type
     */
    private Field getField(String name, LegacySQLTypeName type) {
        return Field.newBuilder(name, type).setMode(Field.Mode.NULLABLE).build();
    }

    /**
     * Verifies that a missing-field schema error triggers adding that column to the table.
     *
     * <p>Given an empty table schema and a {@code "no such field: first_name"} error for a record
     * containing {@code first_name}, when {@code handle} runs, then {@code upsertTable} is invoked once
     * with a single nullable {@code first_name} string field.</p>
     */
    @Test
    public void shouldUpdateTableFieldsOnSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError bigQueryError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> insertErrors = ImmutableMap.of(0L, Collections.singletonList(bigQueryError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = ImmutableList.of(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);

        jsonErrorHandler.handle(insertErrors, records);
        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field firstName = getField("first_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName));
    }

    /**
     * Verifies that non-schema errors do not trigger a table update.
     *
     * <p>Given only generic server errors (no missing-field errors), when {@code handle} runs, then
     * {@code upsertTable} is never invoked.</p>
     */
    @Test
    public void shouldNotUpdateTableWhenNoSchemaError() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError serverError = new BigQueryError("otherresons", "planet eart", "server error");
        BigQueryError anotherError = new BigQueryError("otherresons", "planet eart", "server error");
        Map<Long, List<BigQueryError>> insertErrors = ImmutableMap.of(0L, asList(serverError, anotherError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = ImmutableList.of(validRecord);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(insertErrors, records);

        verify(bigQueryClient, never()).upsertTable(any());

    }

    /**
     * Verifies that missing fields across multiple records are collected into a single update.
     *
     * <p>Given missing-field errors for two records (one carrying {@code first_name}, one carrying
     * {@code last_name}), when {@code handle} runs, then {@code upsertTable} is invoked once with both
     * the {@code first_name} and {@code last_name} fields.</p>
     */
    @Test
    public void shouldUpdateTableFieldsForMultipleRecords() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError firstNameNotFoundError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        BigQueryError anotherError = new BigQueryError("otherresons", "planet eart", "some error");
        BigQueryError lastNameNotFoundError = new BigQueryError("invalid", "first_name", "no such field: last_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, asList(firstNameNotFoundError, anotherError),
                1L, Collections.singletonList(lastNameNotFoundError));


        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithLastName = ImmutableMap.of("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);


        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field firstName = getField("first_name", LegacySQLTypeName.STRING);
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName));
    }

    /**
     * Verifies that records whose errors are not missing-field errors are skipped.
     *
     * <p>Given record {@code 1} with a missing-field error (plus an unrelated error) and record
     * {@code 0} with only an unrelated error, when {@code handle} runs, then {@code upsertTable} is
     * invoked once with only the {@code last_name} field from record {@code 1}.</p>
     */
    @Test
    public void shouldIngoreRecordsWhichHaveOtherErrors() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        BigQueryError otherError = new BigQueryError("otherresons", "planet eart", "server error");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                1L, asList(noSuchFieldError, otherError),
                0L, Collections.singletonList(otherError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("last_name", "john carmack"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    /**
     * Verifies that records without any error entry are skipped.
     *
     * <p>Given a missing-field error only for record {@code 1} (and no entry for record {@code 0}),
     * when {@code handle} runs, then {@code upsertTable} is invoked once with only the
     * {@code last_name} field from record {@code 1}.</p>
     */
    @Test
    public void shouldIngoreRecordsWithNoErrors() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(1L, Collections.singletonList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("last_name", "john carmack"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(lastName));
    }

    /**
     * Verifies that duplicate missing fields are de-duplicated in the update.
     *
     * <p>Given the same missing-field error for three records, two of which carry an identical
     * {@code last_name} column, when {@code handle} runs, then {@code upsertTable} is invoked once with
     * the distinct {@code first_name} and {@code last_name} fields.</p>
     */
    @Test
    public void shouldUpdateOnlyUniqueFields() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithLastName = ImmutableMap.of("last_name", "john carmack");
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithLastName)
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName));
    }

    /**
     * Verifies that missing fields are merged with the existing table schema.
     *
     * <p>Given a non-empty table schema ({@code first_name}, {@code last_name}) and records introducing
     * new columns ({@code newFieldAddress}, {@code newFieldDog}), when {@code handle} runs, then
     * {@code upsertTable} is invoked with both the existing and the new columns.</p>
     */
    @Test
    public void shouldUpdatWithBothMissingFieldsAndExistingTableFields() {
        //existing table fields
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth");
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("newFieldDog", "golden retriever"))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of("newFieldDog", "golden retriever"))
                .build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, bigQuerySinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = getField("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = getField("newFieldAddress", LegacySQLTypeName.STRING);

        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName, newFieldDog, newFieldAddress));
    }

    /**
     * Verifies that a configured partition column keeps its non-string type when added.
     *
     * <p>Given partitioning enabled on {@code event_timestamp_partition} declared as a timestamp
     * default column, when {@code handle} runs over records introducing {@code first_name},
     * {@code last_name} and that partition column, then {@code upsertTable} is invoked with the two
     * string fields plus a timestamp {@code event_timestamp_partition} field.</p>
     */
    @Test
    public void shouldUpsertTableWithPartitionKey() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);


        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError));

        Record validRecordWithFirstName = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        Map<String, Object> columnsMapWithTimestamp = ImmutableMap.of(
                "last_name", "john carmack",
                "event_timestamp_partition", "today's date");
        Record validRecordWithLastName = Record.builder().columns(columnsMapWithTimestamp).build();

        List<Record> validRecords = ImmutableList.of(validRecordWithFirstName, validRecordWithLastName);

        Map<String, String> envMap = ImmutableMap.of(
                "SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE", "true",
                "SINK_BIGQUERY_TABLE_PARTITION_KEY", "event_timestamp_partition",
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp_partition=timestamp");
        BigQuerySinkConfig partitionKeyConfig = ConfigFactory.create(BigQuerySinkConfig.class, envMap);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, partitionKeyConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);


        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        Field firstName = getField("first_name", LegacySQLTypeName.STRING);
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field eventTimestamp = getField("event_timestamp_partition", LegacySQLTypeName.TIMESTAMP);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields, containsInAnyOrder(firstName, lastName, eventTimestamp));
    }

    /**
     * Verifies that disabling string casting for unknown fields raises an error.
     *
     * <p>Given {@code default-datatype-string} disabled and a missing-field error, when {@code handle}
     * runs, then an {@link UnsupportedOperationException} is thrown and {@code upsertTable} is never
     * invoked.</p>
     */
    @Test
    public void shouldThrowExceptionWhenCastFieldsToStringNotTrue() {
        when(bigQueryClient.getSchema()).thenReturn(emptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(0L, Collections.singletonList(noSuchFieldError));

        Record validRecord = Record.builder()
                .columns(ImmutableMap.of("first_name", "john doe"))
                .build();

        List<Record> records = Collections.singletonList(validRecord);
        BigQuerySinkConfig stringDisableConfig = ConfigFactory.create(BigQuerySinkConfig.class, ImmutableMap.of(
                "SINK_BIGQUERY_DEFAULT_DATATYPE_STRING_ENABLE", "false"));
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, stringDisableConfig, instrumentation);
        assertThrows(UnsupportedOperationException.class, () -> jsonErrorHandler.handle(errorInfoMap, records));

        verify(bigQueryClient, never()).upsertTable(any());
    }

    /**
     * Verifies that missing metadata columns are added alongside other missing fields.
     *
     * <p>Given metadata column types for {@code message_offset} and {@code load_time} and records whose
     * metadata carries those values, when {@code handle} runs, then {@code upsertTable} is invoked with
     * the existing fields, the new columns and the integer/timestamp metadata fields.</p>
     */
    @Test
    public void shouldUpdateMissingMetadataFields() {
        //existing table fields
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth",
                "message_offset", 111,
                "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
                "newFieldDog", "golden retriever",
                "load_time", new DateTime(System.currentTimeMillis()),
                "message_offset", 11);
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotherRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotherRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = getField("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = getField("newFieldAddress", LegacySQLTypeName.STRING);

        Field messageOffset = getField("message_offset", LegacySQLTypeName.INTEGER);
        Field loadTime = getField("load_time", LegacySQLTypeName.TIMESTAMP);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(messageOffset, loadTime, firstName, lastName, newFieldDog, newFieldAddress));
    }

    /**
     * Verifies that missing metadata columns and configured default columns are both added.
     *
     * <p>Given metadata column types plus a {@code depot} default column, when {@code handle} runs over
     * records that include {@code depot}, then {@code upsertTable} is invoked with the existing fields,
     * the new columns, the metadata fields and the {@code depot} default column.</p>
     */
    @Test
    public void shouldUpdateMissingMetadataFieldsAndDefaultColumns() {
        //existing table fields
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth",
                "depot", 123,
                "message_offset", 111,
                "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
                "newFieldDog", "golden retriever",
                "load_time", new DateTime(System.currentTimeMillis()),
                "message_offset", 11);
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp",
                "SINK_BIGQUERY_DEFAULT_COLUMNS", "event_timestamp_partition=timestamp,depot=integer");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = getField("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = getField("newFieldAddress", LegacySQLTypeName.STRING);

        Field messageOffset = getField("message_offset", LegacySQLTypeName.INTEGER);
        Field loadTime = getField("load_time", LegacySQLTypeName.TIMESTAMP);
        Field depot = getField("depot", LegacySQLTypeName.INTEGER);
        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(messageOffset, loadTime, firstName, lastName, newFieldDog, newFieldAddress, depot));
    }

    /**
     * Verifies that metadata columns are not added when metadata is disabled.
     *
     * <p>Given metadata column types configured but {@code add-metadata} disabled, when {@code handle}
     * runs, then {@code upsertTable} is invoked with only the existing and new non-metadata
     * columns.</p>
     */
    @Test
    public void shouldNotAddMetadataFieldsWhenDisabled() {
        //existing table fields
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth");
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .metadata(ImmutableMap.of(
                        "message_offset", 111,
                        "load_time", new DateTime(System.currentTimeMillis())))
                .build();

        Record validRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of(
                        "newFieldDog", "golden retriever"))
                .metadata(ImmutableMap.of(
                        "load_time", new DateTime(System.currentTimeMillis()),
                        "message_offset", 11))
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(ImmutableMap.of(
                        "newFieldDog", "german sheppperd"))
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap
                .of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "message_offset=integer,load_time=timestamp",
                        "SINK_BIGQUERY_ADD_METADATA_ENABLED", "false");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        jsonErrorHandler.handle(errorInfoMap, validRecords);

        verify(bigQueryClient, times(1)).upsertTable(fieldsArgumentCaptor.capture());

        //missing fields
        Field newFieldDog = getField("newFieldDog", LegacySQLTypeName.STRING);
        Field newFieldAddress = getField("newFieldAddress", LegacySQLTypeName.STRING);

        List<Field> actualFields = fieldsArgumentCaptor.getValue();
        assertThat(actualFields,
                containsInAnyOrder(firstName, lastName, newFieldDog, newFieldAddress));
    }

    /**
     * Verifies that namespaced metadata is rejected by the JSON error handler.
     *
     * <p>Given a non-empty metadata namespace configured, when {@code handle} runs, then an
     * {@link UnsupportedOperationException} is thrown and {@code upsertTable} is never invoked.</p>
     */
    @Test
    public void shouldThrowErrorForNamespacedMetadataNotSupported() {
        //existing table fields
        Field lastName = getField("last_name", LegacySQLTypeName.STRING);
        Field firstName = getField("first_name", LegacySQLTypeName.STRING);

        Schema nonEmptyTableSchema = Schema.of(firstName, lastName);
        when(bigQueryClient.getSchema()).thenReturn(nonEmptyTableSchema);

        BigQueryError noSuchFieldError = new BigQueryError("invalid", "first_name", "no such field: first_name");
        Map<Long, List<BigQueryError>> errorInfoMap = ImmutableMap.of(
                0L, Collections.singletonList(noSuchFieldError),
                1L, Collections.singletonList(noSuchFieldError),
                2L, Collections.singletonList(noSuchFieldError));

        Map<String, Object> columnsMapWithFistName = ImmutableMap.of(
                "first_name", "john doe",
                "newFieldAddress", "planet earth",
                "message_offset", 111);
        Record validRecordWithFirstName = Record.builder()
                .columns(columnsMapWithFistName)
                .build();

        Map<String, Object> columnsMapWithNewFieldDog = ImmutableMap.of(
                "newFieldDog", "golden retriever",
                "load_time", new DateTime(System.currentTimeMillis()));
        Record validRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();
        Record anotheRecordWithLastName = Record.builder()
                .columns(columnsMapWithNewFieldDog)
                .build();

        List<Record> validRecords = asList(validRecordWithFirstName, validRecordWithLastName, anotheRecordWithLastName);

        Map<String, String> config = ImmutableMap.of("SINK_BIGQUERY_METADATA_COLUMNS_TYPES",
                "message_offset=integer,load_time=timestamp",
                "SINK_BIGQUERY_METADATA_NAMESPACE", "hello_world_namespace");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        JsonErrorHandler jsonErrorHandler = new JsonErrorHandler(bigQueryClient, sinkConfig, instrumentation);
        assertThrows(UnsupportedOperationException.class, () -> jsonErrorHandler.handle(errorInfoMap, validRecords));
        verify(bigQueryClient, never()).upsertTable(any());
    }
}

