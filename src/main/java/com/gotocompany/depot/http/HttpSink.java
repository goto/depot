package com.gotocompany.depot.http;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.response.HttpResponseParser;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * {@link Sink} implementation that delivers consumed messages to a downstream HTTP service.
 *
 * <p>This is the runtime entry point of Depot's HTTP sink. For each batch of messages it orchestrates
 * the full publish cycle: a {@link Request} strategy turns the messages into
 * {@link HttpRequestRecord} instances, records that could not be built are reported as errors without
 * being sent, the remaining valid records are dispatched through the {@link HttpSinkClient}, and the
 * resulting HTTP responses are classified into per-message errors by {@link HttpResponseParser}.</p>
 *
 * <p>Failures are surfaced through a {@link SinkResponse} keyed by the original message index rather
 * than by throwing, so that the caller can distinguish per-message outcomes (for example retryable
 * versus non-retryable HTTP status codes). A transport-level {@link IOException} raised while the
 * batch of valid records is being sent is escalated as a {@link SinkException}.</p>
 *
 * <p>Instances are created by {@link HttpSinkFactory} and are intended to be reused across batches.</p>
 *
 * @see Sink
 * @see HttpSinkFactory
 * @see Request
 * @see HttpSinkClient
 * @see HttpResponseParser
 */
public class HttpSink implements Sink {

    /**
     * Transport client that executes the built HTTP requests and wraps each raw response.
     */
    private final HttpSinkClient httpSinkClient;
    /**
     * Strategy that converts incoming messages into {@link HttpRequestRecord} instances, either one
     * per message or one per batch depending on the configured request mode.
     */
    private final Request request;
    /**
     * Set of HTTP status codes (as map keys) that should be treated as retryable when classifying
     * failed responses.
     */
    private final Map<Integer, Boolean> retryStatusCodeRanges;
    /**
     * Logging and metric facade used to record processing counts and request/response diagnostics.
     */
    private final Instrumentation instrumentation;
    /**
     * Set of HTTP status codes (as map keys) for which the originating request should be logged for
     * diagnostics.
     */
    private final Map<Integer, Boolean> requestLogStatusCodeRanges;

    /**
     * Creates an HTTP sink wired with its collaborators and status-code classification ranges.
     *
     * @param httpSinkClient the client used to execute the built HTTP requests
     * @param request the strategy that builds {@link HttpRequestRecord} instances from messages
     * @param retryStatusCodeRanges status codes (as map keys) to classify as retryable failures
     * @param requestLogStatusCodeRanges status codes (as map keys) whose requests should be logged
     * @param instrumentation the logging and metric facade for the sink
     */
    public HttpSink(HttpSinkClient httpSinkClient, Request request, Map<Integer, Boolean> retryStatusCodeRanges, Map<Integer, Boolean> requestLogStatusCodeRanges, Instrumentation instrumentation) {
        this.httpSinkClient = httpSinkClient;
        this.request = request;
        this.retryStatusCodeRanges = retryStatusCodeRanges;
        this.instrumentation = instrumentation;
        this.requestLogStatusCodeRanges = requestLogStatusCodeRanges;
    }

    /**
     * Builds HTTP requests from the given messages, sends the valid ones and returns per-message
     * outcomes.
     *
     * <p>The processing pipeline is:</p>
     * <ul>
     *   <li>delegate to the configured {@link Request} to turn {@code messages} into
     *       {@link HttpRequestRecord} instances;</li>
     *   <li>partition those records into invalid and valid groups using
     *       {@link HttpRequestRecord#isValid()};</li>
     *   <li>register every invalid record's {@link com.gotocompany.depot.error.ErrorInfo} against
     *       each message index it covers, without contacting the service;</li>
     *   <li>if any valid records exist, log them, dispatch them via {@link HttpSinkClient#send(List)},
     *       and translate the responses into errors with
     *       {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)};</li>
     *   <li>accumulate all errors into the returned {@link SinkResponse}.</li>
     * </ul>
     *
     * @param messages the batch of messages to publish to the HTTP service
     * @return a {@link SinkResponse} mapping the index of each failed message to its error details;
     *     empty when every message was delivered successfully
     * @throws SinkException if an {@link IOException} occurs while the valid records are being sent
     */
    @Override
    public SinkResponse pushToSink(List<Message> messages) throws SinkException {
        List<HttpRequestRecord> requests = request.createRecords(messages);
        Map<Boolean, List<HttpRequestRecord>> splitterRecords = requests.stream().collect(Collectors.partitioningBy(HttpRequestRecord::isValid));
        List<HttpRequestRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<HttpRequestRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(invalidRecord -> invalidRecord.forEach(recordIndex -> sinkResponse.addErrors(recordIndex, invalidRecord.getErrorInfo())));
        if (validRecords.size() > 0) {
            instrumentation.logInfo("Processed {} records to Http Service", validRecords.size());
            try {
                for (HttpRequestRecord validRecord : validRecords) {
                    instrumentation.logDebug(validRecord.getRequestString());
                }
                List<HttpSinkResponse> responses = httpSinkClient.send(validRecords);
                Map<Long, ErrorInfo> errorInfoMap = HttpResponseParser.getErrorsFromResponse(validRecords, responses, retryStatusCodeRanges, requestLogStatusCodeRanges, instrumentation);
                errorInfoMap.forEach(sinkResponse::addErrors);
            } catch (IOException e) {
                throw new SinkException("Exception occurred while execute the request ", e);
            }
        }
        return sinkResponse;
    }

    /**
     * Releases resources held by the sink.
     *
     * <p>The HTTP sink holds no resources that require explicit teardown here (the underlying HTTP
     * client lifecycle is managed elsewhere), so this implementation is intentionally a no-op.</p>
     *
     * @throws IOException never thrown by this implementation; declared to satisfy
     *     {@link java.io.Closeable}
     */
    @Override
    public void close() throws IOException {

    }
}
