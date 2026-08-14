package com.gotocompany.depot.http.record;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A single unit of work for the HTTP sink, pairing a built HTTP request with the message indexes it
 * represents.
 *
 * <p>Each record is either <em>valid</em>, meaning it carries a ready-to-execute
 * {@link HttpEntityEnclosingRequestBase}, or <em>invalid</em>, meaning request construction failed
 * and the record instead carries an {@link ErrorInfo} describing why. In single-request mode a record
 * maps to exactly one message index; in batch mode a single valid record may cover many indexes,
 * which is why the indexes are tracked as a set and exposed through {@link Iterable}.</p>
 *
 * <p>The tracked indexes refer to the positions of the originating messages within the batch passed
 * to the sink, allowing the sink to attribute send outcomes (success, retryable error, etc.) back to
 * the correct messages. Iterating a record yields those message indexes.</p>
 *
 * @see com.gotocompany.depot.http.request.Request
 * @see HttpSinkResponse
 * @see ErrorInfo
 */
public class HttpRequestRecord implements Iterable<Integer> {

    /**
     * Indexes of the messages (within the originating batch) that this record represents.
     */
    private final Set<Integer> recordIndexes = new HashSet<>();
    /**
     * Error describing why request construction failed, or {@code null} for a valid record.
     */
    private final ErrorInfo errorInfo;
    /**
     * Whether this record carries an executable request ({@code true}) or an error ({@code false}).
     */
    private final boolean valid;
    /**
     * The built, body-capable HTTP request to execute, or {@code null} for an invalid record.
     */
    private final HttpEntityEnclosingRequestBase httpRequest;


    /**
     * Creates a record with explicit validity, error and request components.
     *
     * <p>This is the canonical constructor delegated to by the convenience constructors; callers
     * normally use {@link #HttpRequestRecord(HttpEntityEnclosingRequestBase)} for valid records or
     * {@link #HttpRequestRecord(ErrorInfo)} for invalid ones.</p>
     *
     * @param errorInfo the error describing a construction failure, or {@code null} when valid
     * @param valid {@code true} if {@code httpRequest} is present and executable
     * @param httpRequest the built HTTP request, or {@code null} when invalid
     */
    public HttpRequestRecord(ErrorInfo errorInfo, boolean valid, HttpEntityEnclosingRequestBase httpRequest) {
        this.errorInfo = errorInfo;
        this.valid = valid;
        this.httpRequest = httpRequest;
    }

    /**
     * Creates a valid record wrapping an executable HTTP request.
     *
     * @param httpRequest the built HTTP request to execute
     */
    public HttpRequestRecord(HttpEntityEnclosingRequestBase httpRequest) {
        this(null, true, httpRequest);
    }

    /**
     * Creates an invalid record carrying the error that prevented request construction.
     *
     * @param errorInfo the error describing why the message could not be turned into a request
     */
    public HttpRequestRecord(ErrorInfo errorInfo) {
        this(errorInfo, false, null);
    }


    /**
     * Executes the wrapped HTTP request and returns the wrapped response.
     *
     * @param httpClient the client used to execute the request
     * @param instrumentation the logging and metric facade passed to the resulting response wrapper
     * @return the {@link HttpSinkResponse} describing the outcome of the call
     * @throws IOException if the request cannot be executed at the transport level
     */
    public HttpSinkResponse send(HttpClient httpClient, Instrumentation instrumentation) throws IOException {
        HttpResponse response = httpClient.execute(httpRequest);
        return new HttpSinkResponse(response, instrumentation);
    }

    /**
     * Returns the request body as a string by consuming the request entity.
     *
     * @return the serialized request body
     * @throws IOException if the entity cannot be read
     */
    public String getRequestBody() throws IOException {
        return EntityUtils.toString(httpRequest.getEntity());
    }

    /**
     * Associates an additional message index with this record.
     *
     * @param index the index of a message (within the batch) covered by this record
     */
    public void addIndex(Integer index) {
        recordIndexes.add(index);
    }

    /**
     * Associates a collection of message indexes with this record.
     *
     * <p>Used in batch mode where a single valid record covers every message whose body was
     * successfully built.</p>
     *
     * @param indexes the message indexes (within the batch) covered by this record
     */
    public void addAllIndexes(Set<Integer> indexes) {
        recordIndexes.addAll(indexes);
    }

    /**
     * Returns the error describing why this record is invalid.
     *
     * @return the {@link ErrorInfo}, or {@code null} if this record is valid
     */
    public ErrorInfo getErrorInfo() {
        return errorInfo;
    }

    /**
     * Indicates whether this record carries an executable request rather than an error.
     *
     * @return {@code true} if the record is valid and can be sent; {@code false} otherwise
     */
    public boolean isValid() {
        return valid;
    }

    /**
     * Builds a multi-line, human-readable description of the wrapped request for logging.
     *
     * <p>The description includes the request method, URI and headers, and appends the request body
     * only when the request actually carries an entity.</p>
     *
     * @return a formatted string describing the method, URL, headers and (optionally) body
     * @throws IOException if the request body cannot be read while assembling the description
     */
    public String getRequestString() throws IOException {
        StringBuilder requestString = new StringBuilder()
                .append("\nRequest Method: ").append(httpRequest.getMethod())
                .append("\nRequest Url: ").append(httpRequest.getURI())
                .append("\nRequest Headers: ").append(Arrays.asList(httpRequest.getAllHeaders()));
        if (httpRequest.getEntity() != null) {
            requestString.append("\nRequest Body: ").append(getRequestBody());
        }
        return requestString.toString();
    }

    /**
     * Returns an iterator over the message indexes covered by this record.
     *
     * <p>This enables callers to iterate the record directly (for example to attribute a single HTTP
     * response to every message it represents). The iteration order follows that of the backing
     * {@link HashSet} and is therefore unspecified.</p>
     *
     * @return an iterator over the associated message indexes
     */
    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        return recordIndexes.iterator();
    }
}
