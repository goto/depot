package com.gotocompany.depot.bigquery.storage;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.RowError;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import io.grpc.Status.Code;
import com.google.rpc.Status;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Interprets BigQuery Storage Write API responses and exceptions, mapping them onto Depot's
 * {@link SinkResponse} error model.
 *
 * <p>When a batch is appended through the Storage Write API the outcome can take several forms: a
 * stream-level error on the {@code AppendRowsResponse}, per-row errors within that response, or an
 * exception thrown while awaiting the append future. This parser centralises the logic for all three
 * cases, classifies each failure as a retryable ({@code 5xx}) or non-retryable ({@code 4xx}) error,
 * records the failure against the originating input message index, logs it and increments error
 * metrics.</p>
 *
 * <p>The static helpers expose the gRPC/BigQuery status mapping rules (for example
 * {@link #getError(com.google.rpc.Status)} and {@link #shouldRetry(io.grpc.Status)}) while the
 * instance methods apply those rules to a concrete {@link BigQueryPayload} and {@link SinkResponse}.
 * Because invalid records are dropped while serializing the payload, the parser relies on
 * {@link BigQueryPayload#getInputIndex(long)} to translate BigQuery row indexes back to the original
 * batch positions.</p>
 *
 * @see BigQueryPayload
 * @see com.gotocompany.depot.error.ErrorInfo
 */
public class BigQueryStorageResponseParser {
    /**
     * gRPC status codes that are considered transient and therefore safe to retry.
     *
     * <p>Failures with any of these codes are reported as {@link ErrorType#SINK_5XX_ERROR} so the
     * affected messages can be retried by the sink.</p>
     */
    private static final Set<Code> RETRYABLE_ERROR_CODES =
            new HashSet<Code>() {{
                add(Code.INTERNAL);
                add(Code.ABORTED);
                add(Code.CANCELLED);
                add(Code.FAILED_PRECONDITION);
                add(Code.DEADLINE_EXCEEDED);
                add(Code.UNAVAILABLE);
            }};
    /** Sink configuration supplying the table, dataset and project identifiers used when tagging metrics. */
    private final BigQuerySinkConfig sinkConfig;
    /** Instrumentation used to log errors and emit error counters. */
    private final Instrumentation instrumentation;
    /** Provider of BigQuery metric names and tags. */
    private final BigQueryMetrics bigQueryMetrics;

    /**
     * Creates a response parser bound to the given configuration, instrumentation and metrics.
     *
     * @param sinkConfig        the BigQuery sink configuration whose table, dataset and project are
     *                          used to tag emitted error metrics
     * @param instrumentation   the instrumentation used for logging and metric emission
     * @param bigQueryMetrics   the provider of BigQuery metric names and tag templates
     */
    public BigQueryStorageResponseParser(
            BigQuerySinkConfig sinkConfig,
            Instrumentation instrumentation,
            BigQueryMetrics bigQueryMetrics) {
        this.sinkConfig = sinkConfig;
        this.instrumentation = instrumentation;
        this.bigQueryMetrics = bigQueryMetrics;
    }

    /**
     * Maps a BigQuery {@code com.google.rpc.Status} onto an {@link ErrorInfo}, classifying it as a
     * client ({@code 4xx}) or server ({@code 5xx}) error.
     *
     * <p>The numeric status code is resolved to a {@code com.google.rpc.Code} and grouped as follows:</p>
     * <ul>
     *     <li>{@code OK} yields {@code null} (no error).</li>
     *     <li>Client-side codes such as {@code INVALID_ARGUMENT}, {@code NOT_FOUND},
     *     {@code PERMISSION_DENIED} or {@code FAILED_PRECONDITION} map to
     *     {@link ErrorType#SINK_4XX_ERROR}.</li>
     *     <li>Server-side or transient codes such as {@code INTERNAL}, {@code UNAVAILABLE} or
     *     {@code DEADLINE_EXCEEDED} map to {@link ErrorType#SINK_5XX_ERROR}.</li>
     *     <li>Any unrecognised code maps to {@link ErrorType#SINK_UNKNOWN_ERROR}.</li>
     * </ul>
     *
     * @param error the BigQuery status to classify
     * @return an {@link ErrorInfo} carrying the status message and the resolved error type, or
     *         {@code null} when the status is {@code OK}
     */
    public static ErrorInfo getError(Status error) {
        com.google.rpc.Code code = com.google.rpc.Code.forNumber(error.getCode());
        switch (code) {
            case OK:
                return null;
            case CANCELLED:
            case INVALID_ARGUMENT:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case PERMISSION_DENIED:
            case UNAUTHENTICATED:
            case RESOURCE_EXHAUSTED:
            case FAILED_PRECONDITION:
            case ABORTED:
            case OUT_OF_RANGE:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_4XX_ERROR);
            case UNKNOWN:
            case INTERNAL:
            case DATA_LOSS:
            case UNAVAILABLE:
            case UNIMPLEMENTED:
            case UNRECOGNIZED:
            case DEADLINE_EXCEEDED:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_5XX_ERROR);
            default:
                return new ErrorInfo(new Exception(error.getMessage()), ErrorType.SINK_UNKNOWN_ERROR);
        }
    }

    /**
     * Determines whether a gRPC status should be retried.
     *
     * @param status the gRPC status describing the failure
     * @return {@code true} if the status code is one of the {@linkplain #RETRYABLE_ERROR_CODES
     *         retryable codes}, {@code false} otherwise
     */
    public static boolean shouldRetry(io.grpc.Status status) {
        return BigQueryStorageResponseParser.RETRYABLE_ERROR_CODES.contains(status.getCode());
    }

    /**
     * Wraps a per-row {@code RowError} into a non-retryable {@link ErrorInfo}.
     *
     * @param rowError the row-level error reported by BigQuery for a single appended row
     * @return an {@link ErrorInfo} carrying the row error message and classified as
     *         {@link ErrorType#SINK_4XX_ERROR}
     */
    public static ErrorInfo get4xxError(RowError rowError) {
        return new ErrorInfo(new Exception(rowError.getMessage()), ErrorType.SINK_4XX_ERROR);
    }

    /**
     * Builds a synthetic {@code AppendRowsResponse} carrying a {@code FAILED_PRECONDITION} error.
     *
     * <p>This is returned by the writer when an append is attempted on a permanently closed client,
     * so that downstream handling treats every row in the batch as failed without contacting BigQuery.</p>
     *
     * @return an {@code AppendRowsResponse} whose error code corresponds to {@code FAILED_PRECONDITION}
     */
    public static AppendRowsResponse get4xxErrorResponse() {
        return AppendRowsResponse.newBuilder().setError(Status.newBuilder().setCode(com.google.rpc.Code.FAILED_PRECONDITION.ordinal()).build()).build();
    }

    /**
     * Records errors for messages that failed conversion before they were ever sent to BigQuery.
     *
     * <p>Iterates over the per-record metadata in the payload and, for every record flagged invalid,
     * adds its captured {@link ErrorInfo} to the sink response (keyed by the original input index) and
     * logs the failure together with the offending message's metadata.</p>
     *
     * @param payload      the payload whose per-record metadata is inspected for invalid records
     * @param messages     the original input batch, used to resolve metadata for logging
     * @param sinkResponse the sink response that accumulates the errors
     */
    public void setSinkResponseForInvalidMessages(
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse) {

        payload.forEach(meta -> {
            if (!meta.isValid()) {
                sinkResponse.addErrors(meta.getInputIndex(), meta.getErrorInfo());
                instrumentation.logError(
                        "Error {} occurred while converting to payload for record {}",
                        meta.getErrorInfo(),
                        messages.get((int) meta.getInputIndex()).getMetadataString());
            }
        });
    }

    /**
     * Increments the BigQuery total-errors counter, tagged with the table, dataset, project and the
     * supplied error descriptor.
     *
     * @param error an object whose {@link Object#toString()} identifies the error category recorded in
     *              the metric tag (for example a status code or error enum)
     */
    private void instrumentErrors(Object error) {
        instrumentation.incrementCounter(
                bigQueryMetrics.getBigqueryTotalErrorsMetrics(),
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, sinkConfig.getTableName()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, sinkConfig.getDatasetName()),
                String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, sinkConfig.getGCloudProjectID()),
                String.format(BigQueryMetrics.BIGQUERY_ERROR_TAG, error.toString()));
    }

    /**
     * Records errors reported within a successful (non-exceptional) append response.
     *
     * <p>Handles two distinct error channels carried by an {@code AppendRowsResponse}:</p>
     * <ul>
     *     <li>A <em>stream-level</em> error (when {@code appendRowsResponse.hasError()} is true): every
     *     valid row index in the payload is marked as failed with the classified
     *     {@link ErrorInfo} and the error metric is incremented per row.</li>
     *     <li><em>Per-row</em> errors (the {@code RowErrorsList}): each row error is mapped to a
     *     non-retryable {@link ErrorType#SINK_4XX_ERROR}, attributed to the corresponding input
     *     message and logged together with that message's metadata.</li>
     * </ul>
     *
     * @param payload           the payload whose index mapping resolves BigQuery row indexes to input
     *                          indexes
     * @param appendRowsResponse the response returned by the Storage Write API append call
     * @param messages          the original input batch, used to resolve metadata for logging
     * @param sinkResponse      the sink response that accumulates the errors
     */
    public void setSinkResponseForErrors(
            BigQueryPayload payload,
            AppendRowsResponse appendRowsResponse,
            List<Message> messages,
            SinkResponse sinkResponse) {
        if (appendRowsResponse.hasError()) {
            instrumentation.logError("received an error in stream :{} ", appendRowsResponse.getError());
            com.google.rpc.Status error = appendRowsResponse.getError();
            ErrorInfo errorInfo = BigQueryStorageResponseParser.getError(error);
            Set<Long> payloadIndexes = payload.getPayloadIndexes();
            com.google.rpc.Code code = com.google.rpc.Code.forNumber(error.getCode());
            payloadIndexes.forEach(index -> {
                long inputIndex = payload.getInputIndex(index);
                sinkResponse.addErrors(inputIndex, errorInfo);
                instrumentErrors(code);
            });
        }

        //per message error
        List<RowError> rowErrorsList = appendRowsResponse.getRowErrorsList();
        rowErrorsList.forEach(rowError -> {
            ErrorInfo errorInfo = BigQueryStorageResponseParser.get4xxError(rowError);
            long inputIndex = payload.getInputIndex(rowError.getIndex());
            sinkResponse.addErrors(inputIndex, errorInfo);
            String metadataString = messages.get((int) inputIndex).getMetadataString();
            instrumentation.logError(
                    "Error {} occurred while sending the payload for record {} with RowError {}",
                    errorInfo,
                    metadataString,
                    rowError);
            instrumentErrors(rowError.getCode());
        });
    }

    /**
     * Records errors when the append operation fails by throwing, rather than returning an error
     * response.
     *
     * <p>The handling depends on the type of the throwable:</p>
     * <ul>
     *     <li>For an {@code Exceptions.AppendSerializationError}, all rows are first marked as
     *     retryable ({@link ErrorType#SINK_5XX_ERROR}); then the rows whose indexes appear in the
     *     serialization error's row-index-to-message map are overridden as non-retryable
     *     ({@link ErrorType#SINK_4XX_ERROR}), since those rows cannot be serialized and must not be
     *     retried.</li>
     *     <li>For any other throwable, the gRPC status is inspected: if {@link #shouldRetry(io.grpc.Status)}
     *     is true every row is marked {@link ErrorType#SINK_5XX_ERROR}, otherwise every row is marked
     *     {@link ErrorType#SINK_4XX_ERROR}.</li>
     * </ul>
     *
     * @param cause        the throwable raised while appending or awaiting the append result
     * @param payload      the payload whose index mapping resolves row indexes to input indexes
     * @param messages     the original input batch, used to resolve metadata for logging
     * @param sinkResponse the sink response that accumulates the errors
     */
    public void setSinkResponseForException(
            Throwable cause,
            BigQueryPayload payload,
            List<Message> messages,
            SinkResponse sinkResponse) {
        io.grpc.Status status = io.grpc.Status.fromThrowable(cause);
        instrumentation.logError("Error from exception: {} ", status);
        if (cause instanceof Exceptions.AppendSerializationError) {
            // first set all messages to retryable
            IntStream.range(0, payload.getPayloadIndexes().size())
                    .forEach(index -> {
                        sinkResponse.addErrors(payload.getInputIndex(index), new ErrorInfo(new Exception(cause), ErrorType.SINK_5XX_ERROR));
                        instrumentErrors(status.getCode());
                    });
            // then set non retryable messages
            Exceptions.AppendSerializationError ase = (Exceptions.AppendSerializationError) cause;
            Map<Integer, String> rowIndexToErrorMessage = ase.getRowIndexToErrorMessage();
            rowIndexToErrorMessage.forEach((index, err) -> {
                long inputIndex = payload.getInputIndex(index);
                String metadataString = messages.get((int) inputIndex).getMetadataString();
                ErrorInfo errorInfo = new ErrorInfo(new Exception(err), ErrorType.SINK_4XX_ERROR);
                instrumentation.logError(
                        "Error {} occurred while sending the payload for record {}",
                        errorInfo,
                        metadataString);
                sinkResponse.addErrors(inputIndex, errorInfo);
                instrumentErrors(BigQueryMetrics.BigQueryStorageAPIError.ROW_APPEND_ERROR);
            });
        } else {
            if (BigQueryStorageResponseParser.shouldRetry(status)) {
                IntStream.range(0, payload.getPayloadIndexes().size())
                        .forEach(index -> {
                            sinkResponse.addErrors(payload.getInputIndex(index), new ErrorInfo(new Exception(cause), ErrorType.SINK_5XX_ERROR));
                            instrumentErrors(status.getCode());
                        });
            } else {
                IntStream.range(0, payload.getPayloadIndexes().size())
                        .forEach(index -> {
                            sinkResponse.addErrors(payload.getInputIndex(index), new ErrorInfo(new Exception(cause), ErrorType.SINK_4XX_ERROR));
                            instrumentErrors(status.getCode());
                        });
            }
        }
    }
}
