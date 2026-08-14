package com.gotocompany.depot.bigquery.json;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverter;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure;
import com.gotocompany.depot.bigquery.proto.BigqueryFields;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link DepotStencilUpdateListener} that prepares a BigQuery table for JSON-sourced data.
 *
 * <p>When the sink consumes JSON whose schema is inferred from incoming data, this
 * listener seeds the destination table with the configured default columns and metadata
 * columns and installs a {@link MessageRecordConverter} into the shared cache. Only
 * dynamically inferred schemas are supported; stencil-based JSON schemas are not yet
 * implemented.</p>
 *
 * @see com.gotocompany.depot.bigquery.handler.JsonErrorHandler
 * @see com.gotocompany.depot.bigquery.BigqueryStencilUpdateListenerFactory
 */
public class BigqueryJsonUpdateListener extends DepotStencilUpdateListener {
    /** Cache that holds the converter used to turn messages into BigQuery records. */
    private final MessageRecordConverterCache converterCache;
    /** Sink configuration supplying default columns, metadata settings and partitioning. */
    private final BigQuerySinkConfig config;
    /** Client used to read the existing schema and upsert the prepared table schema. */
    private final BigQueryClient bigQueryClient;
    /** Instrumentation used to log and report schema update failures. */
    private final Instrumentation instrumentation;

    /**
     * Creates the listener and validates that dynamic schema inference is enabled.
     *
     * @param config          the sink configuration; dynamic schema inference must be
     *                        enabled
     * @param converterCache  the cache that will receive the message-to-record converter
     * @param bigQueryClient  the client used to read and upsert the table schema
     * @param instrumentation the instrumentation used to log schema update failures
     * @throws UnsupportedOperationException if dynamic schema inference is disabled, since
     *                                       stencil-based JSON schemas are not supported
     */
    public BigqueryJsonUpdateListener(BigQuerySinkConfig config, MessageRecordConverterCache converterCache, BigQueryClient bigQueryClient, Instrumentation instrumentation) {
        this.converterCache = converterCache;
        this.config = config;
        this.bigQueryClient = bigQueryClient;
        this.instrumentation = instrumentation;
        if (!config.getSinkBigqueryDynamicSchemaEnable()) {
            throw new UnsupportedOperationException("currently only schema inferred from incoming data is supported, stencil schema support for json will be added in future");
        }
    }

    /**
     * Builds the BigQuery table schema for JSON data and upserts it.
     *
     * <p>Creates and caches a fresh {@link MessageRecordConverter}, then assembles the set
     * of fields to apply from the configured default columns and, when enabled, the
     * metadata columns. The existing table fields are merged in before the table is
     * upserted, so existing columns are preserved.</p>
     *
     * @throws UnsupportedOperationException if metadata is enabled together with a
     *                                       non-empty metadata namespace (nested JSON is
     *                                       unsupported), or if a partition column is not
     *                                       of a supported type
     * @throws IllegalArgumentException if a configured column type name is not a valid
     *                                  BigQuery type, or if the same field is declared in
     *                                  both the default-column and metadata configuration
     * @throws com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure if updating the
     *                                  BigQuery table fails
     */
    @Override
    public void updateSchema() {
        MessageParser parser = getMessageParser();
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(parser, config);
        converterCache.setMessageRecordConverter(messageRecordConverter);
        List<TupleString> defaultColumns = config.getSinkBigqueryDefaultColumns();
        HashSet<Field> fieldsToBeUpdated = defaultColumns
                .stream()
                .map(this::getField)
                .collect(Collectors.toCollection(HashSet::new));
        if (config.shouldAddMetadata() && !config.getBqMetadataNamespace().isEmpty()) {
            throw new UnsupportedOperationException("metadata namespace is not supported, because nested json structure is not supported");
        }
        addMetadataFields(fieldsToBeUpdated, defaultColumns);
        try {
            Schema existingTableSchema = bigQueryClient.getSchema();
            FieldList existingTableFields = existingTableSchema.getFields();
            existingTableFields.iterator().forEachRemaining(fieldsToBeUpdated::add);
            bigQueryClient.upsertTable(new ArrayList<>(fieldsToBeUpdated));
        } catch (BigQueryException e) {
            String errMsg = "Error while updating bigquery table in json update listener:" + e.getMessage();
            instrumentation.logError(errMsg);
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    /*
    throws error incase there are duplicate fields between metadata and default columns config
     */
    /**
     * Adds the configured metadata fields to the set of fields to be applied.
     *
     * <p>Only acts when metadata is enabled. Resolves the strict metadata fields and
     * verifies that none of them collides with a configured default column before adding
     * them.</p>
     *
     * @param fieldsToBeUpdated the mutable set of fields being assembled for the table;
     *                          metadata fields are added to it
     * @param defaultColumns    the configured default columns, used to detect name clashes
     * @throws IllegalArgumentException if a metadata field name is also present among the
     *                                  default columns
     */
    private void addMetadataFields(HashSet<Field> fieldsToBeUpdated, List<TupleString> defaultColumns) {
        if (config.shouldAddMetadata()) {
            Set<String> defaultColumnNames = defaultColumns
                    .stream()
                    .map(TupleString::getFirst)
                    .collect(Collectors.toSet());
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            List<Field> metadataFields = BigqueryFields.getMetadataFieldsStrict(metadataColumnsTypes);
            Optional<Field> duplicateField = metadataFields
                    .stream()
                    .filter(m -> defaultColumnNames.contains(m.getName())).findFirst();
            if (duplicateField.isPresent()) {
                String duplicateFieldName = duplicateField.get().getName();
                instrumentation.logError("duplicate key found in default columns and metadata config {}", duplicateFieldName);
                throw new IllegalArgumentException("duplicate field called "
                        + duplicateFieldName
                        + " is present in both default columns config and metadata config");
            }
            fieldsToBeUpdated.addAll(metadataFields);
        }
    }

    /**
     * Converts a configured column tuple into a BigQuery field definition.
     *
     * @param tupleString a tuple of the column name and its BigQuery type name
     * @return the corresponding BigQuery {@link Field}
     * @throws IllegalArgumentException if the type name is not a valid BigQuery type
     * @throws UnsupportedOperationException if the column is the partition key but its type
     *                                       is neither {@code DATE} nor {@code TIMESTAMP}
     */
    private Field getField(TupleString tupleString) {
        String fieldName = tupleString.getFirst();
        LegacySQLTypeName fieldDataType = LegacySQLTypeName.valueOfStrict(tupleString.getSecond().toUpperCase());
        return checkAndCreateField(fieldName, fieldDataType);
    }

    /**
     * Range BigQuery partitioning is not supported, supported partition fields have to be of DATE or TIMESTAMP type..
     *
     * <p>When table partitioning is disabled the field is created directly. When
     * partitioning is enabled and the field is the configured partition key, its type must
     * be {@code DATE} or {@code TIMESTAMP}; range-based BigQuery partitioning is not
     * supported.</p>
     *
     * @param fieldName     the name of the field to create
     * @param fieldDataType the BigQuery type of the field
     * @return a nullable BigQuery {@link Field} for the given name and type
     * @throws UnsupportedOperationException if the field is the partition key but its type
     *                                       is neither {@code DATE} nor {@code TIMESTAMP}
     */
    private Field checkAndCreateField(String fieldName, LegacySQLTypeName fieldDataType) {
        Boolean isPartitioningEnabled = config.isTablePartitioningEnabled();
        if (!isPartitioningEnabled) {
            return Field.newBuilder(fieldName, fieldDataType).setMode(Field.Mode.NULLABLE).build();
        }
        String partitionKey = config.getTablePartitionKey();
        boolean isValidPartitionDataType = (fieldDataType == LegacySQLTypeName.TIMESTAMP || fieldDataType == LegacySQLTypeName.DATE);
        if (partitionKey.equals(fieldName) && !isValidPartitionDataType) {
            throw new UnsupportedOperationException("supported partition fields have to be of DATE or TIMESTAMP type..");
        }
        return Field.newBuilder(fieldName, fieldDataType).setMode(Field.Mode.NULLABLE).build();
    }
}
