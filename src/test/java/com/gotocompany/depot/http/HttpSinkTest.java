package com.gotocompany.depot.http;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.SinkException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link HttpSink}, the {@link com.gotocompany.depot.Sink} implementation that
 * publishes batches of messages to a downstream HTTP service.
 *
 * <p>The suite drives the {@link HttpSink#pushToSink(List)} pipeline in isolation by mocking the
 * sink's collaborators with Mockito: the {@link Request} that converts messages into
 * {@link HttpRequestRecord} instances, the {@link HttpSinkClient} that dispatches the valid records,
 * and the {@link Instrumentation} facade used for logging. Real {@link HttpSinkResponse} objects wrap
 * mocked Apache HttpClient responses so that status-code driven error classification is verified end
 * to end.</p>
 *
 * <p>Records are fabricated through {@link #createRecord(Integer, ErrorInfo, boolean)} so that valid
 * and invalid records, together with explicit message indexes, can be combined to assert how
 * successes, deserialization failures and HTTP error responses are surfaced through the returned
 * {@link SinkResponse}.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTest {

    /**
     * Mocked request strategy that converts the input messages into {@link HttpRequestRecord}
     * instances under test control.
     */
    @Mock
    private Request request;

    /**
     * Mocked transport client whose {@code send} result drives the response-parsing branch of the
     * sink.
     */
    @Mock
    private HttpSinkClient httpSinkClient;

    /**
     * Mocked Apache HttpClient response wrapped by the {@link HttpSinkResponse} fixtures.
     */
    @Mock
    private HttpResponse response;

    /**
     * Mocked response entity returned for failed responses so the body can be inspected during
     * error classification.
     */
    @Mock
    private HttpEntity httpEntity;

    /**
     * Mocked status line supplying the HTTP status code used to classify each response.
     */
    @Mock
    private StatusLine statusLine;

    /**
     * Mocked logging and metric facade used to verify the diagnostic logging performed by the sink.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Verifies that a batch consisting entirely of valid records that receive {@code 200} responses
     * yields a {@link SinkResponse} reporting no errors.
     *
     * <p>Given five valid records and a client that returns five success responses, when
     * {@link HttpSink#pushToSink(List)} is invoked, then {@link SinkResponse#hasErrors()} reports
     * {@code false}.</p>
     *
     * @throws IOException never in practice; declared because the mocked collaborators expose checked
     *     transport methods
     */
    @Test
    public void shouldPushToSink() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        when(request.createRecords(messages)).thenReturn(records);
        when(httpSinkClient.send(records)).thenReturn(responses);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertFalse(sinkResponse.hasErrors());
    }

    /**
     * Verifies that records flagged invalid during construction are reported as errors without being
     * sent to the service.
     *
     * <p>Given five records of which the records at indexes {@code 0} and {@code 2} carry a
     * {@link ErrorType#DESERIALIZATION_ERROR} and are marked invalid, when the batch is pushed, then
     * only the three valid records are dispatched and the {@link SinkResponse} contains exactly two
     * deserialization errors keyed by the original message indexes {@code 0} and {@code 2}.</p>
     *
     * @throws IOException never in practice; declared because the mocked collaborators expose checked
     *     transport methods
     */
    @Test
    public void shouldReportParsingErrors() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR), false));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR), false));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));
        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        when(request.createRecords(messages)).thenReturn(records);
        List<HttpRequestRecord> validRecords = records.stream().filter(HttpRequestRecord::isValid).collect(Collectors.toList());
        when(httpSinkClient.send(validRecords)).thenReturn(responses);
        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
    }

    /**
     * Verifies that valid records receiving a server-side {@code 500} response are reported as
     * non-retryable server errors when {@code 500} is not within the configured retry ranges.
     *
     * <p>Given five valid records, a client returning failed responses, and retry ranges containing
     * {@code 400}, {@code 499} and {@code 501} (but not {@code 500}), when the batch is pushed, then
     * the {@link SinkResponse} contains five errors and the entries for indexes {@code 1}, {@code 3}
     * and {@code 4} are classified as {@link ErrorType#SINK_5XX_ERROR}.</p>
     *
     * @throws IOException never in practice; declared because the mocked collaborators expose checked
     *     transport methods
     */
    @Test
    public void shouldReportErrors() throws IOException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));
        records.add(createRecord(1, null, true));
        records.add(createRecord(2, null, true));
        records.add(createRecord(3, null, true));
        records.add(createRecord(4, null, true));

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(response.getEntity()).thenReturn(httpEntity);
        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));
        responses.add(new HttpSinkResponse(response, instrumentation));

        when(request.createRecords(messages)).thenReturn(records);
        List<HttpRequestRecord> validRecords = records.stream().filter(HttpRequestRecord::isValid).collect(Collectors.toList());
        when(httpSinkClient.send(validRecords)).thenReturn(responses);
        when(httpSinkClient.send(records)).thenReturn(responses);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(400, true);
        retryStatusCodeRanges.put(499, true);
        retryStatusCodeRanges.put(501, true);

        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        SinkResponse sinkResponse = httpSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(5, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(1).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(3).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, sinkResponse.getErrorsFor(4).getErrorType());
    }

    /**
     * Verifies the diagnostic logging performed by the sink when it has valid records to send.
     *
     * <p>Given a single valid record, when the batch is pushed, then the sink logs the processed
     * record count exactly once via {@link Instrumentation#logInfo(String, Object...)} and logs the
     * full request description (method, URL, headers and body) exactly once at debug level via
     * {@link Instrumentation#logDebug(String)}.</p>
     *
     * @throws SinkException never in practice; declared because {@link HttpSink#pushToSink(List)} may
     *     escalate transport failures as a sink exception
     */
    @Test
    public void shouldLogRequestBodyInDebugMode() throws SinkException {
        List<Message> messages = new ArrayList<>();
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0, null, true));

        when(request.createRecords(messages)).thenReturn(records);

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();

        retryStatusCodeRanges.put(501, true);
        HttpSink httpSink = new HttpSink(httpSinkClient, request, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);

        when(request.createRecords(messages)).thenReturn(records);

        httpSink.pushToSink(messages);
        verify(instrumentation, times(1)).logInfo("Processed {} records to Http Service",
                1);
        verify(instrumentation, times(1)).logDebug("\nRequest Method: PUT\nRequest Url: http://dummy.com\nRequest Headers: [Accept: text/plain]\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]");

    }


    /**
     * Builds a {@link HttpRequestRecord} fixture wrapping an {@code HTTP PUT} to {@code http://dummy.com}
     * with a small JSON entity and an {@code Accept} header, associated with a single message index.
     *
     * @param index the message index (within the batch) the record represents
     * @param errorInfo the error to attach, or {@code null} for a record without a construction error
     * @param isValid {@code true} to mark the record as a sendable valid record; {@code false}
     *     otherwise
     * @return the constructed record carrying the supplied error, validity flag and index
     */
    private HttpRequestRecord createRecord(Integer index, ErrorInfo errorInfo, boolean isValid) {
        HttpEntityEnclosingRequestBase httpRequest = new HttpPut("http://dummy.com");
        httpRequest.setEntity(new StringEntity("[{\"key\":\"value1\"},{\"key\":\"value2\"}]", ContentType.APPLICATION_JSON));
        httpRequest.setHeader(new BasicHeader("Accept", "text/plain"));
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, isValid, httpRequest);
        record.addIndex(index);
        return record;
    }

    /**
     * Produces the request-logging status-code range map used by the sink fixtures, covering HTTP
     * status codes {@code 400} through {@code 600} via {@link RangeToHashMapConverter}.
     *
     * @return a map whose keys are the status codes for which originating requests should be logged
     */
    private Map<Integer, Boolean> createRequestLogStatusCode() {
        return new RangeToHashMapConverter().convert(null, "400-600");
    }

}
