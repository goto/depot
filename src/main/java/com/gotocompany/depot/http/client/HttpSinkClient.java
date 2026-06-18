package com.gotocompany.depot.http.client;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.client.HttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin transport layer that executes built HTTP requests and emits per-response metrics.
 *
 * <p>The client owns a shared Apache {@link HttpClient} and dispatches a list of already-built
 * {@link HttpRequestRecord} instances sequentially, collecting each outcome as an
 * {@link HttpSinkResponse}. After every call it records a response-code metric so that downstream
 * dashboards can track the distribution of HTTP status codes returned by the target service.</p>
 *
 * <p>Instances are created by {@link com.gotocompany.depot.http.HttpSinkFactory} and reused across
 * batches; the wrapped {@link HttpClient} is expected to be thread-safe and connection-pooled.</p>
 *
 * @see HttpRequestRecord
 * @see HttpSinkResponse
 * @see HttpSinkMetrics
 */
public class HttpSinkClient {

    /**
     * Shared, connection-pooled HTTP client used to execute every request.
     */
    private final HttpClient httpClient;
    /**
     * Logging and metric facade used to emit the response-code counter.
     */
    private final Instrumentation instrumentation;
    /**
     * Provider of metric names specific to the HTTP sink, such as the response-code counter.
     */
    private final HttpSinkMetrics httpSinkMetrics;

    /**
     * Creates a client wrapping the given HTTP client, metrics provider and instrumentation.
     *
     * @param httpClient the HTTP client used to execute requests
     * @param httpSinkMetrics the provider of HTTP sink metric names
     * @param instrumentation the logging and metric facade
     */
    public HttpSinkClient(HttpClient httpClient, HttpSinkMetrics httpSinkMetrics, Instrumentation instrumentation) {
        this.httpClient = httpClient;
        this.instrumentation = instrumentation;
        this.httpSinkMetrics = httpSinkMetrics;
    }

    /**
     * Executes the given request records in order and returns their wrapped responses.
     *
     * <p>Each record is dispatched through {@link HttpRequestRecord#send(HttpClient, Instrumentation)}
     * with the shared client, the resulting {@link HttpSinkResponse} is appended to the result list,
     * and a response-code metric is captured via {@link #instrument(HttpSinkResponse)}. The returned
     * list is positionally aligned with {@code records}, which lets the caller correlate each response
     * back to its originating record.</p>
     *
     * @param records the HTTP request records to execute
     * @return the list of responses, in the same order as {@code records}
     * @throws IOException if executing any request fails at the transport level
     */
    public List<HttpSinkResponse> send(List<HttpRequestRecord> records) throws IOException {
        List<HttpSinkResponse> responseList = new ArrayList<>();
        for (HttpRequestRecord record : records) {
            HttpSinkResponse sinkResponse = record.send(httpClient, instrumentation);
            responseList.add(sinkResponse);
            instrument(sinkResponse);
        }
        return responseList;
    }

    /**
     * Records a counter metric tagged with the response's HTTP status code.
     *
     * <p>The status code is read from the response and turned into a {@code status_code=} tag; when
     * the code is negative (indicating no status line was available) the tag is left without a numeric
     * value. The {@link HttpSinkMetrics#getHttpResponseCodeTotalMetric()} counter is then incremented
     * by one with that tag.</p>
     *
     * @param httpSinkResponse the response whose status code is being counted
     */
    private void instrument(HttpSinkResponse httpSinkResponse) {
        int statusCode = httpSinkResponse.getResponseCode();
        String httpCodeTag = statusCode < 0 ? "status_code=" : "status_code=" + statusCode;
        instrumentation.captureCount(httpSinkMetrics.getHttpResponseCodeTotalMetric(), 1L, httpCodeTag);

    }

}
