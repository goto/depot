package com.gotocompany.depot.http.response;

import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Immutable-after-construction wrapper around a raw {@link HttpResponse} exposing the facts the sink
 * needs.
 *
 * <p>On construction it derives, in order, whether the call failed, its numeric status code, whether
 * the response body should be logged, and (conditionally) the body text. Success is defined as a
 * {@code 2xx} status code; when the response carries no status line the code is normalized to
 * {@code -1} and the call is treated as a failure, signalling a transport-level problem. The response
 * body is only materialized when it is needed (the response is a failure or debug logging is enabled);
 * otherwise the entity is quietly consumed to release the connection.</p>
 *
 * @see com.gotocompany.depot.http.record.HttpRequestRecord#send(org.apache.http.client.HttpClient, Instrumentation)
 * @see HttpResponseParser
 */
public class HttpSinkResponse {
    /**
     * Regular expression matching HTTP success status codes, namely any code beginning with
     * {@code "2"}.
     */
    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    /**
     * Whether the response represents a failure (non-{@code 2xx} or no status line).
     */
    private boolean isFail;
    /**
     * The HTTP status code, or {@code -1} when no status line was present.
     */
    private int responseCode;
    /**
     * The response body text, populated only when it should be logged or the call failed.
     */
    private String responseBody;
    /**
     * Whether the response body should be logged, mirroring whether debug logging is enabled.
     */
    private boolean shouldLogResponse;

    /**
     * Wraps a raw HTTP response, deriving the failure flag, status code, log decision and body.
     *
     * <p>The fields are computed in dependency order: the failure flag and status code are derived
     * first, then the debug-driven {@link #shouldLogResponse} flag, and finally the body, whose
     * materialization depends on the earlier two.</p>
     *
     * @param response the raw HTTP response to wrap; may be {@code null} or lack a status line
     * @param instrumentation the logging facade consulted to decide whether to capture the body
     * @throws IOException if the response body needs to be read but cannot be
     */
    public HttpSinkResponse(HttpResponse response, Instrumentation instrumentation) throws IOException {
        setIsFail(response);
        setResponseCode(response);
        setShouldLogResponse(instrumentation);
        setResponseBody(response);
    }

    /**
     * Derives and stores whether the response represents a failure.
     *
     * <p>Defaults to failure and, when a status line is present, flips to success only if the status
     * code matches {@link #SUCCESS_CODE_PATTERN} (a {@code 2xx} code). A missing status line therefore
     * remains a failure.</p>
     *
     * @param response the raw HTTP response being inspected
     */
    private void setIsFail(HttpResponse response) {
        isFail = true;
        if (hasStatusLine(response)) {
            isFail = !Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches();
        }
    }

    /**
     * Derives and stores the numeric status code, normalizing a missing status line to {@code -1}.
     *
     * @param response the raw HTTP response being inspected
     */
    private void setResponseCode(HttpResponse response) {
        if (hasStatusLine(response)) {
            responseCode = response.getStatusLine().getStatusCode();
        } else {
            responseCode = -1;
        }
    }

    /**
     * Returns whether the response carries a usable status line.
     *
     * @param response the raw HTTP response to test; may be {@code null}
     * @return {@code true} if {@code response} and its status line are both non-{@code null}
     */
    private static boolean hasStatusLine(HttpResponse response) {
        return response != null && response.getStatusLine() != null;
    }

    /**
     * Reads or discards the response entity according to the log and failure flags.
     *
     * <p>When there is no entity nothing happens. When the response should be logged or has failed,
     * the entity is read into {@link #responseBody}; otherwise it is quietly consumed to free the
     * underlying connection without retaining the body.</p>
     *
     * @param response the raw HTTP response whose entity is handled
     * @throws IOException if the entity must be read but cannot be converted to a string
     */
    private void setResponseBody(HttpResponse response) throws IOException {
        if (!hasResponse(response)) {
            return;
        }
        if (shouldLogResponse || isFail) {
            responseBody = EntityUtils.toString(response.getEntity());
        } else {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

    /**
     * Returns the captured response body.
     *
     * @return the response body text, or {@code null} if it was not captured
     */
    public String getResponseBody() {
        return responseBody;
    }

    /**
     * Returns the HTTP status code of the response.
     *
     * @return the status code, or {@code -1} if the response had no status line
     */
    public int getResponseCode() {
        return responseCode;
    }

    /**
     * Returns whether the response is considered a failure.
     *
     * @return {@code true} if the status code is not {@code 2xx} or no status line was present
     */
    public boolean isFail() {
        return isFail;
    }

    /**
     * Returns whether the response body is flagged for logging.
     *
     * @return {@code true} if the body should be logged
     */
    public boolean shouldLogResponse() {
        return shouldLogResponse;
    }

    /**
     * Sets whether the response body should be logged, mirroring the debug logging state.
     *
     * @param instrumentation the logging facade whose debug-enabled state drives the flag
     */
    public void setShouldLogResponse(Instrumentation instrumentation) {
        shouldLogResponse = instrumentation.isDebugEnabled();
    }

    /**
     * Returns whether the response carries an entity that can be read.
     *
     * @param response the raw HTTP response to test; may be {@code null}
     * @return {@code true} if {@code response} and its entity are both non-{@code null}
     */
    private boolean hasResponse(HttpResponse response) {
        return Objects.nonNull(response) && Objects.nonNull(response.getEntity());
    }
}
