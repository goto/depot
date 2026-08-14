package com.gotocompany.depot.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigquery.client.BigQueryClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/*
the job of the class is to handle unknown field errors and then update the bq table schema,
 this happens incase of where schema is inferred from incoming data
 */
/**
 * {@link ErrorHandler} that repairs the BigQuery table schema when unknown-field errors occur.
 *
 * <p>This handler is used when the destination table schema is inferred from incoming
 * JSON data rather than from a registered protobuf schema. When BigQuery rejects rows
 * because they reference columns that do not yet exist in the table ("no such field"
 * errors), the handler derives the missing columns from the failed records and upserts
 * the table so that subsequent inserts can succeed.</p>
 *
 * <p>Column types are resolved from the metadata-column configuration and the
 * default-column configuration; otherwise, when casting of all columns to string is
 * enabled, the column is created as a {@code STRING}. A non-empty BigQuery metadata
 * namespace (nested JSON) is not supported.</p>
 *
 * @see ErrorHandlerFactory
 * @see com.gotocompany.depot.bigquery.json.BigqueryJsonUpdateListener
 */
public class JsonErrorHandler implements ErrorHandler {

    /** Client used to read the current table schema and upsert added columns. */
    private final BigQueryClient bigQueryClient;
    /** Whether columns with no explicit type mapping are created as {@code STRING}. */
    private final boolean castAllColumnsToStringDataType;
    /** Configured metadata column names mapped to their BigQuery type names. */
    private final Map<String, String> metadataColumnsTypesMap;
    /** Namespace for nested metadata columns; nested JSON is unsupported when non-empty. */
    private final String bqMetadataNamespace;
    /** Instrumentation used to log schema-repair activity. */
    private final Instrumentation instrumentation;
    /** Configured default column names mapped to their BigQuery type names. */
    private final Map<String, String> defaultColumnsMap;

    /**
     * Builds a handler from the BigQuery client, sink configuration and instrumentation.
     *
     * <p>Captures the default-column and (when metadata is enabled) metadata-column type
     * mappings from the configuration, the metadata namespace, and whether all columns
     * should default to the {@code STRING} type. When metadata is disabled the metadata
     * type map is left empty.</p>
     *
     * @param bigQueryClient     the client used to read and upsert the table schema
     * @param bigQuerySinkConfig the sink configuration providing default columns, metadata
     *                           columns, the metadata namespace and the string-default flag
     * @param instrumentation    the instrumentation used to log schema-repair activity
     */
    public JsonErrorHandler(BigQueryClient bigQueryClient, BigQuerySinkConfig bigQuerySinkConfig, Instrumentation instrumentation) {

        this.instrumentation = instrumentation;
        this.bigQueryClient = bigQueryClient;
        defaultColumnsMap = bigQuerySinkConfig.getSinkBigqueryDefaultColumns()
                .stream()
                .collect(Collectors.toMap(TupleString::getFirst, TupleString::getSecond));
        castAllColumnsToStringDataType = bigQuerySinkConfig.getSinkBigqueryDefaultDatatypeStringEnable();
        bqMetadataNamespace = bigQuerySinkConfig.getBqMetadataNamespace();
        if (!bigQuerySinkConfig.shouldAddMetadata()) {
            metadataColumnsTypesMap = Collections.emptyMap();
        } else {
            metadataColumnsTypesMap = bigQuerySinkConfig
                    .getMetadataColumnsTypes()
                    .stream()
                    .collect(Collectors.toMap(TupleString::getFirst, TupleString::getSecond));
        }

    }

    /**
     * Repairs the table schema for rows that failed with unknown-field errors.
     *
     * <p>Implements {@link ErrorHandler#handle(Map, List)}. Reads the current table
     * schema, finds the insert errors that correspond to unknown fields, and from the
     * records that triggered those errors collects the column names that are not yet part
     * of the schema. Each such column is converted to a BigQuery
     * {@link com.google.cloud.bigquery.Field} via {@link #getField(String)},
     * de-duplicated, combined with the existing fields, and the table is upserted. When
     * there are no unknown-field errors the method does nothing.</p>
     *
     * @param insertErrors a map from the index of a failed row within the batch to the
     *                     {@link BigQueryError} instances reported for that row
     * @param records      the records that were submitted, used to resolve the column
     *                     names of the rows that failed
     */
    public void handle(Map<Long, List<BigQueryError>> insertErrors, List<Record> records) {

        Schema schema = bigQueryClient.getSchema();
        FieldList existingFieldList = schema.getFields();
        List<Entry<Long, List<BigQueryError>>> unknownFieldBqErrors = getUnknownFieldBqErrors(insertErrors);
        if (!unknownFieldBqErrors.isEmpty()) {
            ArrayList<Field> bqSchemaFields = unknownFieldBqErrors
                    .stream()
                    .map(x -> getColumnNamesForRecordsWhichHadUnknownBqFieldErrors(records, x))
                    .flatMap(Collection::stream)
                    .filter(key -> filterExistingFields(existingFieldList, key))
                    .map(this::getField)
                    .distinct()
                    .collect(Collectors.toCollection(ArrayList::new));
            instrumentation.logInfo("updating table with missing fields detected {}", bqSchemaFields);
            existingFieldList.iterator().forEachRemaining(bqSchemaFields::add);
            bigQueryClient.upsertTable(bqSchemaFields);
        }
    }

    /**
     * Returns the column names present on the record that produced the given error entry.
     *
     * <p>Uses the entry's key as the index into {@code records} and returns the key set of
     * that record's columns.</p>
     *
     * @param records the records submitted in the batch
     * @param x       a map entry whose key is the index of the failed record and whose
     *                value is the list of errors reported for it
     * @return the set of column names declared by the failed record
     */
    private Set<String> getColumnNamesForRecordsWhichHadUnknownBqFieldErrors(List<Record> records, Entry<Long, List<BigQueryError>> x) {
        int recordKey = x.getKey().intValue();
        return records.get(recordKey).getColumns().keySet();
    }


    /**
     * Filters the insert errors down to those caused by unknown fields.
     *
     * @param insertErrors a map from failed-row index to the errors reported for that row
     * @return the entries that contain at least one "no such field" error
     */
    private List<Entry<Long, List<BigQueryError>>> getUnknownFieldBqErrors(Map<Long, List<BigQueryError>> insertErrors) {
        return insertErrors.entrySet().stream()
                .filter((x) -> {
                    List<BigQueryError> value = x.getValue();
                    List<BigQueryError> bqErrorsWithNoSuchFields = getBqErrorsWithNoSuchFields(value);
                    return !bqErrorsWithNoSuchFields.isEmpty();
                }).collect(Collectors.toList());
    }

    /**
     * Selects the BigQuery errors that denote an unknown column.
     *
     * @param value the errors reported for a single failed row
     * @return the errors whose reason is {@code "invalid"} and whose message contains
     *         {@code "no such field"}
     */
    private List<BigQueryError> getBqErrorsWithNoSuchFields(List<BigQueryError> value) {
        return value.stream()
                .filter(bigQueryError -> bigQueryError.getReason().equals("invalid") && bigQueryError.getMessage().contains("no such field")
                ).collect(Collectors.toList());
    }

    /**
     * This method only used for unknown fields.
     *
     * <p>Builds the BigQuery field definition for a previously unknown column. Resolution order: a
     * configured metadata column type, then a configured default column type, and finally the
     * {@code STRING} type when casting all columns to string is enabled. The created field is always
     * {@code NULLABLE}.</p>
     *
     * @param key the name of the column to create
     * @return a nullable BigQuery {@link com.google.cloud.bigquery.Field} for the column
     * @throws UnsupportedOperationException if a metadata namespace is configured (nested
     *                                       JSON is unsupported), or if the column has no
     *                                       metadata or default type mapping and casting
     *                                       all columns to string is disabled
     */

    private Field getField(String key) {
        if (!bqMetadataNamespace.isEmpty()) {
            throw new UnsupportedOperationException("metadata namespace is not supported, because nested json structure is not supported");
        }
        if (metadataColumnsTypesMap.containsKey(key)) {
            return Field.newBuilder(key, LegacySQLTypeName.valueOfStrict(metadataColumnsTypesMap.get(key).toUpperCase()))
                    .setMode(Field.Mode.NULLABLE)
                    .build();
        }
        if (defaultColumnsMap.containsKey(key)) {
            return Field.newBuilder(key, LegacySQLTypeName.valueOfStrict(defaultColumnsMap.get(key).toUpperCase()))
                    .setMode(Field.Mode.NULLABLE)
                    .build();
        }
        if (!castAllColumnsToStringDataType) {
            throw new UnsupportedOperationException("only string data type is supported for fields other than partition key");
        }
        return Field.newBuilder(key, LegacySQLTypeName.STRING)
                .setMode(Field.Mode.NULLABLE)
                .build();
    }

    /**
     * Returns whether a column is absent from the existing schema.
     *
     * <p>Probes the existing field list for the given name; treats a successful lookup as
     * "already present" (filtered out) and an {@link IllegalArgumentException} from the
     * lookup as "missing" (retained).</p>
     *
     * @param existingFieldList the fields already present in the table schema
     * @param key               the candidate column name
     * @return {@code true} if the column is not present in {@code existingFieldList} and
     *         therefore needs to be added, {@code false} otherwise
     */
    private boolean filterExistingFields(FieldList existingFieldList, String key) {
        try {
            existingFieldList.get(key);
            return false;
        } catch (IllegalArgumentException ex) {
            return true;
        }
    }

}
