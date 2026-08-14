package com.gotocompany.depot.bigtable.parser;

import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import com.gotocompany.depot.bigtable.model.BigTableRecord;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.BigTableMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.bigtable.response.BigTableResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates failed Bigtable mutations into Depot's classified per-record error model.
 *
 * <p>After a bulk write partially fails, {@code BigTableResponseParser} maps each
 * {@link com.google.cloud.bigtable.data.v2.models.MutateRowsException.FailedMutation} back to the
 * originating message index and produces an {@link ErrorInfo} whose {@link ErrorType} reflects the
 * nature of the failure: a retryable error becomes {@link ErrorType#SINK_RETRYABLE_ERROR}, otherwise
 * the HTTP status class selects {@link ErrorType#SINK_4XX_ERROR} or {@link ErrorType#SINK_5XX_ERROR},
 * and anything else becomes {@link ErrorType#SINK_UNKNOWN_ERROR}. It also increments Bigtable error
 * counters tagged by failure kind and logs the details of each failure.</p>
 *
 * @see BigTableResponse
 * @see com.gotocompany.depot.bigtable.BigTableSink
 */
public class BigTableResponseParser {
    /**
     * Maps the failed mutations in a Bigtable response to classified per-message errors.
     *
     * <p>For every {@link com.google.cloud.bigtable.data.v2.models.MutateRowsException.FailedMutation}
     * the offending {@link BigTableRecord} is located by the failure's position in the batch and its
     * original message index is used as the result key. The failure is classified into an
     * {@link ErrorType}: a retryable error yields {@link ErrorType#SINK_RETRYABLE_ERROR}; otherwise an
     * HTTP status code beginning with {@code 4} yields {@link ErrorType#SINK_4XX_ERROR}, one beginning
     * with {@code 5} yields {@link ErrorType#SINK_5XX_ERROR}, and any other code yields
     * {@link ErrorType#SINK_UNKNOWN_ERROR}. Each failure is logged with its metadata and cause, and a
     * Bigtable error counter is incremented, tagged according to the error details (bad request, quota
     * failure, precondition failure, or RPC failure when details are absent).</p>
     *
     * @param validRecords the records that were submitted to Bigtable, indexed as they were sent
     * @param bigTableResponse the response holding the failed mutations to classify
     * @param bigtableMetrics the metric names and tags used when incrementing error counters
     * @param instrumentation the logging and metrics facade used to log failures and emit counters
     * @return a map from each failed message's index to its classified {@link ErrorInfo}
     */
    public static Map<Long, ErrorInfo> getErrorsFromSinkResponse(List<BigTableRecord> validRecords, BigTableResponse bigTableResponse, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        HashMap<Long, ErrorInfo> errorInfoMap = new HashMap<>();
        for (MutateRowsException.FailedMutation fm : bigTableResponse.getFailedMutations()) {
            BigTableRecord record = validRecords.get(fm.getIndex());
            long messageIndex = record.getIndex();

            String httpStatusCode = String.valueOf(fm.getError().getStatusCode().getCode().getHttpStatusCode());
            if (fm.getError().isRetryable()) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (httpStatusCode.startsWith("4")) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_4XX_ERROR));
            } else if (httpStatusCode.startsWith("5")) {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_5XX_ERROR));
            } else {
                errorInfoMap.put(messageIndex, new ErrorInfo(fm.getError(), ErrorType.SINK_UNKNOWN_ERROR));
            }

            instrumentation.logError("Error while inserting to Bigtable. Record Metadata: {}, Cause: {}, Reason: {}, StatusCode: {}, HttpCode: {}",
                    record.getMetadata(),
                    fm.getError().getCause(),
                    fm.getError().getReason(),
                    fm.getError().getStatusCode().getCode(),
                    fm.getError().getStatusCode().getCode().getHttpStatusCode());

            if (fm.getError().getErrorDetails() == null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
            } else if (fm.getError().getErrorDetails().getBadRequest() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.BAD_REQUEST));
            } else if (fm.getError().getErrorDetails().getQuotaFailure() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.QUOTA_FAILURE));
            } else if (fm.getError().getErrorDetails().getPreconditionFailure() != null) {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.PRECONDITION_FAILURE));
            } else {
                instrumentation.incrementCounter(bigtableMetrics.getBigtableTotalErrorsMetrics(), String.format(BigTableMetrics.BIGTABLE_ERROR_TAG, BigTableMetrics.BigTableErrorType.RPC_FAILURE));
            }
        }
        return errorInfoMap;
    }
}
