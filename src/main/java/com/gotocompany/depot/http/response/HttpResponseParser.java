package com.gotocompany.depot.http.response;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Translates HTTP responses into per-message {@link ErrorInfo} entries, classifying failures.
 *
 * <p>After the {@link com.gotocompany.depot.http.client.HttpSinkClient} executes a batch of records,
 * this parser walks the positionally-aligned records and responses, logs request and response
 * diagnostics according to the configured status-code ranges, and, for every failed response, maps
 * each covered message index to an {@link ErrorInfo} whose {@link ErrorType} reflects the failure
 * class. Successful responses contribute no errors.</p>
 *
 * <p>Classification precedence for a failed status code is: configured retryable range first
 * ({@link ErrorType#SINK_RETRYABLE_ERROR}), then the {@code 4xx} range
 * ({@link ErrorType#SINK_4XX_ERROR}), then the {@code 5xx} range
 * ({@link ErrorType#SINK_5XX_ERROR}), and finally anything else as
 * {@link ErrorType#SINK_UNKNOWN_ERROR}. The class exposes only static methods.</p>
 *
 * @see HttpSinkResponse
 * @see ErrorInfo
 * @see ErrorType
 */
public class HttpResponseParser {
    /**
     * Lower bound (inclusive) of the HTTP client-error ({@code 4xx}) range.
     */
    private static final int MIN_BAD_REQUEST_CODE = 400;
    /**
     * Upper bound (inclusive) of the HTTP client-error ({@code 4xx}) range.
     */
    private static final int MAX_BAD_REQUEST_CODE = 499;
    /**
     * Lower bound (inclusive) of the HTTP server-error ({@code 5xx}) range.
     */
    private static final int MIN_SERVER_ERROR_CODE = 500;
    /**
     * Upper bound (inclusive) of the HTTP server-error ({@code 5xx}) range.
     */
    private static final int MAX_SERVER_ERROR_CODE = 599;

    /**
     * Builds a map of failed message indexes to their classified errors from a batch of responses.
     *
     * <p>The {@code records} and {@code responses} lists are expected to be positionally aligned. For
     * each pair the response code is logged; the request is additionally logged when
     * {@link #shouldLogRequest(int, Map)} holds, and the response body is logged at debug level when
     * the response opted in. For failed responses the covered message indexes are classified via
     * {@link #getErrors(HttpRequestRecord, int, Map)} and merged into the result, and an error line is
     * logged with the status code and body.</p>
     *
     * @param records the request records that were sent, aligned with {@code responses}
     * @param responses the responses received, aligned with {@code records}
     * @param retryStatusCodeRanges status codes (as map keys) to classify as retryable
     * @param requestLogStatusCodeRanges status codes (as map keys) whose request should be logged
     * @param instrumentation the logging facade used for diagnostics
     * @return a map from each failed message index to its {@link ErrorInfo}; empty when all responses
     *     succeeded
     * @throws IOException if reading a request string for logging fails
     */
    public static Map<Long, ErrorInfo> getErrorsFromResponse(
            List<HttpRequestRecord> records,
            List<HttpSinkResponse> responses,
            Map<Integer, Boolean> retryStatusCodeRanges,
            Map<Integer, Boolean> requestLogStatusCodeRanges,
            Instrumentation instrumentation) throws IOException {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (int i = 0; i < responses.size(); i++) {
            HttpRequestRecord record = records.get(i);
            HttpSinkResponse response = responses.get(i);
            int responseCode = response.getResponseCode();
            instrumentation.logInfo("Response Status: {}", responseCode);
            if (shouldLogRequest(responseCode, requestLogStatusCodeRanges)) {
                instrumentation.logInfo(record.getRequestString());
            }
            if (response.shouldLogResponse()) {
                instrumentation.logDebug(response.getResponseBody());
            }
            if (response.isFail()) {
                errors.putAll(getErrors(record, responseCode, retryStatusCodeRanges));
                instrumentation.logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", responseCode, response.getResponseBody());
            }
        }
        return errors;
    }

    /**
     * Classifies a single failed response into an {@link ErrorInfo} for every message it covers.
     *
     * <p>Each message index covered by the record (obtained by iterating the record) is mapped to an
     * {@link ErrorInfo} wrapping a synthetic exception carrying the status code. The
     * {@link ErrorType} is chosen by precedence: configured retryable code
     * ({@link ErrorType#SINK_RETRYABLE_ERROR}), {@code 4xx} ({@link ErrorType#SINK_4XX_ERROR}),
     * {@code 5xx} ({@link ErrorType#SINK_5XX_ERROR}), otherwise
     * {@link ErrorType#SINK_UNKNOWN_ERROR}.</p>
     *
     * @param record the failed request record whose covered message indexes are classified
     * @param responseCode the HTTP status code returned for the record
     * @param retryStatusCodeRanges status codes (as map keys) to classify as retryable
     * @return a map from each covered message index to its classified {@link ErrorInfo}
     */
    private static Map<Long, ErrorInfo> getErrors(HttpRequestRecord record, int responseCode, Map<Integer, Boolean> retryStatusCodeRanges) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (long messageIndex : record) {
            if (retryStatusCodeRanges.containsKey(responseCode)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_RETRYABLE_ERROR));
            } else if (isResponseCodeInRange(responseCode, MIN_BAD_REQUEST_CODE, MAX_BAD_REQUEST_CODE)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_4XX_ERROR));
            } else if (isResponseCodeInRange(responseCode, MIN_SERVER_ERROR_CODE, MAX_SERVER_ERROR_CODE)) {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_5XX_ERROR));
            } else {
                errors.put(messageIndex, new ErrorInfo(new Exception("Error:" + responseCode), ErrorType.SINK_UNKNOWN_ERROR));
            }
        }
        return errors;
    }

    /**
     * Returns whether a status code falls within an inclusive range.
     *
     * @param responseCode the HTTP status code to test
     * @param minRange the inclusive lower bound of the range
     * @param maxRange the inclusive upper bound of the range
     * @return {@code true} if {@code responseCode} lies within {@code [minRange, maxRange]}
     */
    private static boolean isResponseCodeInRange(int responseCode, int minRange, int maxRange) {
        return responseCode >= minRange && responseCode <= maxRange;
    }

    /**
     * Returns whether the originating request should be logged for the given status code.
     *
     * <p>Logging is requested when the status code is {@code -1} (no status line was available,
     * indicating a transport-level problem) or when the code is present in the configured
     * request-log ranges.</p>
     *
     * @param responseCode the HTTP status code of the response
     * @param requestLogStatusCodeRanges status codes (as map keys) whose request should be logged
     * @return {@code true} if the request should be logged
     */
    private static boolean shouldLogRequest(int responseCode, Map<Integer, Boolean> requestLogStatusCodeRanges) {
        return responseCode == -1 || requestLogStatusCodeRanges.containsKey(responseCode);
    }
}
