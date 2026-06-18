package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.gotocompany.depot.bigquery.exception.BigQuerySinkException;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigquery.error.ErrorDescriptor;
import com.gotocompany.depot.bigquery.error.ErrorParser;
import com.gotocompany.depot.bigquery.error.InvalidSchemaError;
import com.gotocompany.depot.bigquery.error.OOBError;
import com.gotocompany.depot.bigquery.error.StoppedError;
import com.gotocompany.depot.bigquery.error.UnknownError;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates BigQuery legacy streaming insert responses into Depot's {@link ErrorInfo} error model.
 *
 * <p>This parser is used by the legacy {@code insertAll} (table data streaming) path. It inspects the
 * {@link InsertAllResponse} returned for a batch, maps each reported {@link BigQueryError} to a Depot
 * {@link ErrorDescriptor} via {@link ErrorParser}, classifies the failure and produces a map from the
 * failing message index to an {@link ErrorInfo}. Each classified error category is also recorded as a
 * BigQuery error counter.</p>
 *
 * @see ErrorParser
 * @see com.gotocompany.depot.error.ErrorType
 */
public class BigQueryResponseParser {
    /**
     * Parses the {@link InsertAllResponse} object and returns errors type {@link ErrorDescriptor}.
     * {@link InsertAllResponse} in bqResponse are 1 to 1 indexed based on the records that are requested to be inserted.
     *
     * <p>If the response reports no errors an empty map is returned. Otherwise,
     * for each failing entry the corresponding record is resolved, its low-level BigQuery errors are
     * parsed and the first matching category determines the resulting {@link ErrorInfo}:</p>
     * <ul>
     *     <li>{@link UnknownError} maps to {@link ErrorType#SINK_UNKNOWN_ERROR}.</li>
     *     <li>{@link InvalidSchemaError} and {@link OOBError} map to {@link ErrorType#SINK_4XX_ERROR}.</li>
     *     <li>{@link StoppedError} maps to {@link ErrorType#SINK_5XX_ERROR}.</li>
     * </ul>
     * Each detected category also increments the corresponding BigQuery error counter, and the failure
     * is logged together with the record's columns and metadata.
     *
     * @param records    - list of records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @param bigQueryMetrics - provider of BigQuery metric names used when incrementing error counters
     * @param instrumentation - instrumentation used to log failures and emit error metrics
     * @return list of messages with error.
     */
    public static Map<Long, ErrorInfo> getErrorsFromBQResponse(
            final List<Record> records,
            final InsertAllResponse bqResponse,
            BigQueryMetrics bigQueryMetrics,
            Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errorInfoResponse = new HashMap<>();
        if (!bqResponse.hasErrors()) {
            return errorInfoResponse;
        }
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (final Map.Entry<Long, List<BigQueryError>> errorEntry : insertErrorsMap.entrySet()) {
            Record record = records.get(errorEntry.getKey().intValue());
            long messageIndex = record.getIndex();
            List<ErrorDescriptor> errors = ErrorParser.parseError(errorEntry.getValue());
            instrumentation.logError("Error while bigquery insert for message. \nRecord: {}, \nError: {}, \nMetaData: {}",
                    record.getColumns(), errors, record.getMetadata());

            if (errorMatch(errors, UnknownError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.UNKNOWN_ERROR));
            } else if (errorMatch(errors, InvalidSchemaError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.INVALID_SCHEMA_ERROR));
            } else if (errorMatch(errors, OOBError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.OOB_ERROR));
            } else if (errorMatch(errors, StoppedError.class)) {
                errorInfoResponse.put(messageIndex, new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR));
                instrumentation.incrementCounter(bigQueryMetrics.getBigqueryTotalErrorsMetrics(), String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, BigQueryMetrics.BigQueryErrorType.STOPPED_ERROR));
            }
        }
        return errorInfoResponse;
    }

    /**
     * Determines whether any descriptor in the list is exactly of the given class.
     *
     * @param errors the parsed error descriptors to test
     * @param c      the {@link ErrorDescriptor} class to match against (exact class equality)
     * @return {@code true} if at least one descriptor's runtime class equals {@code c}, {@code false}
     *         otherwise
     */
    private static boolean errorMatch(List<ErrorDescriptor> errors, Class c) {
        return errors.stream().anyMatch(errorDescriptor -> errorDescriptor.getClass().equals(c));
    }
}
