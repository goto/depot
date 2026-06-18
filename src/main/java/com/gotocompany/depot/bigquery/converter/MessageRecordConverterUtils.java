package com.gotocompany.depot.bigquery.converter;

import com.google.api.client.util.DateTime;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.utils.DateUtils;
import com.gotocompany.depot.message.MessageUtils;

import java.util.List;
import java.util.Map;

/**
 * Helper methods for enriching BigQuery column maps with message metadata and a synthetic timestamp.
 *
 * <p>Used by {@link MessageRecordConverter} while building the column map for a record. It centralises
 * two cross-cutting concerns: appending configured metadata columns (optionally namespaced) and, for
 * the JSON data type, adding a current-time event timestamp column.</p>
 *
 * @see MessageRecordConverter
 */
public class MessageRecordConverterUtils {

    /** Name of the column populated with the current UTC time for JSON records when enabled. */
    public static final String JSON_TIME_STAMP_COLUMN = "event_timestamp";

    /**
     * Adds configured metadata columns to the column map, when metadata enrichment is enabled.
     *
     * <p>When {@code config.shouldAddMetadata()} is {@code true}, the configured metadata columns are
     * read from the message and any timestamp-typed metadata is wrapped in a
     * {@link com.google.api.client.util.DateTime}. If no metadata namespace is configured the metadata
     * entries are merged directly into the columns; otherwise they are nested under the configured
     * namespace key. When metadata enrichment is disabled the column map is left unchanged.</p>
     *
     * @param columns the mutable column map to enrich
     * @param message the message whose metadata is read
     * @param config  the sink configuration providing the metadata toggle, column types and namespace
     */
    public static void addMetadata(Map<String, Object> columns, Message message, BigQuerySinkConfig config) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
            Map<String, Object> finalMetadata = MessageUtils.checkAndSetTimeStampColumns(
                    metadata,
                    metadataColumnsTypes,
                    (DateTime::new));
            if (config.getBqMetadataNamespace().isEmpty()) {
                columns.putAll(finalMetadata);
            } else {
                columns.put(config.getBqMetadataNamespace(), finalMetadata);
            }

        }
    }

    /**
     * Adds an {@value #JSON_TIME_STAMP_COLUMN} column with the current UTC time for JSON records.
     *
     * <p>The column is added only when the configured schema data type is JSON and the event-timestamp
     * feature is enabled; otherwise the column map is left unchanged.</p>
     *
     * @param columns the mutable column map to enrich
     * @param config  the sink configuration providing the data type and event-timestamp toggle
     */
    public static void addTimeStampColumnForJson(Map<String, Object> columns, BigQuerySinkConfig config) {
        if (config.getSinkConnectorSchemaDataType() == SinkConnectorSchemaDataType.JSON
                && config.getSinkBigqueryAddEventTimestampEnable()) {
            columns.put(JSON_TIME_STAMP_COLUMN, DateUtils.formatCurrentTimeAsUTC());
        }
    }
}

