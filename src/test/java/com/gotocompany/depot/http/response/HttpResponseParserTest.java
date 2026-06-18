package com.gotocompany.depot.http.response;

import com.gotocompany.depot.config.converter.RangeToHashMapConverter;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.http.record.HttpRequestRecord;
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
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link HttpResponseParser}, which classifies a batch of HTTP responses into a map of
 * failed message indexes to {@link ErrorInfo} and logs request/response diagnostics.
 *
 * <p>Real {@link HttpRequestRecord} fixtures (built by {@link #createRecord(Integer)}) are paired with
 * a mix of real {@link HttpSinkResponse} wrappers around mocked Apache HttpClient responses and, in
 * one scenario, mocked {@link HttpSinkResponse} instances. The {@link Instrumentation} facade is a
 * Mockito mock so that the request and error logging can be verified. The tests cover error
 * classification ({@code 4xx}, {@code 5xx} and configured retryable codes), the empty result when no
 * response fails, and the conditional request logging governed by the request-log status-code
 * ranges.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpResponseParserTest {

    /**
     * Mocked logging and metric facade; verified for the request and error logging the parser emits.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Mocked entity attached to failed responses so the response body can be read during parsing.
     */
    @Mock
    private HttpEntity entity;

    /**
     * Verifies that only failed responses contribute server-error entries to the result.
     *
     * <p>Given five records and five responses alternating success ({@code 200}) and failure
     * ({@code 500}) with no configured retry ranges, when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then the result has three {@link ErrorType#SINK_5XX_ERROR} entries (for message indexes
     * {@code 1}, {@code 7} and {@code 12}) and the error line is logged three times.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldGetErrorsFromResponse() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", 500, null);
    }

    /**
     * Verifies that no errors are produced when no response reports a failure.
     *
     * <p>Given five mocked responses whose {@code getResponseCode()} returns {@code 500} but whose
     * {@code isFail()} defaults to {@code false}, when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then the returned error map is empty.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldGetEmptyMapWhenNoErrors() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        List<HttpSinkResponse> responses = new ArrayList<>();
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));
        responses.add(mock(HttpSinkResponse.class));

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn(500)
        );
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertTrue(errors.isEmpty());
    }

    /**
     * Verifies that the originating request is logged when the response code is within the request-log
     * ranges.
     *
     * <p>Given three failed ({@code 500}) responses and a request-log range of {@code 401-600}, when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then three {@link ErrorType#SINK_5XX_ERROR} entries (for message indexes {@code 0},
     * {@code 1} and {@code 4}) are produced, the formatted request is logged three times and the error
     * line is logged three times.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldLogRequestIfResponseCodeInStatusCodeRanges() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
        }};
        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(0L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(4L).getErrorType());
        verify(instrumentation, times(3)).logInfo(
                "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]"
        );
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", 500, null);
    }

    /**
     * Verifies that the originating request is not logged when the response code is outside the
     * request-log ranges.
     *
     * <p>Given three failed ({@code 400}) responses and a request-log range of {@code 401-600} (which
     * excludes {@code 400}), when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then three {@link ErrorType#SINK_4XX_ERROR} entries (for message indexes {@code 0},
     * {@code 1} and {@code 4}) are produced and the formatted request is never logged.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldNotLogRequestIfResponseCodeIsNotInStatusCodeRanges() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(400);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
        }};
        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        assertEquals(3, errors.size());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(0L).getErrorType());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(1L).getErrorType());
        assertEquals(ErrorType.SINK_4XX_ERROR, errors.get(4L).getErrorType());
        verify(instrumentation, times(0)).logInfo(
                "\nRequest Method: PUT"
                        + "\nRequest Url: http://dummy.com"
                        + "\nRequest Headers: [Accept: text/plain]"
                        + "\nRequest Body: [{\"key\":\"value1\"},{\"key\":\"value2\"}]"
        );
    }

    /**
     * Verifies that failures are classified as retryable when their status code is in the retry ranges.
     *
     * <p>Given responses alternating success and failure ({@code 500}) and a retry range containing
     * {@code 500}, when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then the three failures (message indexes {@code 1}, {@code 7} and {@code 12}) are
     * classified as {@link ErrorType#SINK_RETRYABLE_ERROR} and the error line is logged three times.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldGetSinkRetryableErrorWhenStatusCodeFallsUnderConfiguredRange() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(500, true);

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", 500, null);
    }

    /**
     * Verifies that failures are not classified as retryable when their status code is outside the
     * retry ranges.
     *
     * <p>Given responses alternating success and failure ({@code 500}) and a retry range containing
     * only {@code 501}, when
     * {@link HttpResponseParser#getErrorsFromResponse(List, List, Map, Map, Instrumentation)} is
     * invoked, then the three failures (message indexes {@code 1}, {@code 7} and {@code 12}) remain
     * {@link ErrorType#SINK_5XX_ERROR} and the error line is logged three times.</p>
     *
     * @throws IOException never in practice; declared because parsing reads request strings
     */
    @Test
    public void shouldNotGetSinkRetryableErrorWhenStatusCodeIsNotUnderConfiguredRange() throws IOException {
        List<HttpRequestRecord> records = new ArrayList<>();
        records.add(createRecord(0));
        records.add(createRecord(1));
        records.add(createRecord(4));
        records.add(createRecord(7));
        records.add(createRecord(12));

        HttpResponse successHttpResponse = mock(HttpResponse.class);
        StatusLine successStatusLine = mock(StatusLine.class);
        when(successHttpResponse.getStatusLine()).thenReturn(successStatusLine);
        when(successStatusLine.getStatusCode()).thenReturn(200);

        HttpResponse failedHttpResponse = mock(HttpResponse.class);
        StatusLine failedStatusLine = mock(StatusLine.class);
        when(failedHttpResponse.getStatusLine()).thenReturn(failedStatusLine);
        when(failedStatusLine.getStatusCode()).thenReturn(500);
        when(failedHttpResponse.getEntity()).thenReturn(entity);
        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(successHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
            add(new HttpSinkResponse(failedHttpResponse, instrumentation));
        }};

        Map<Integer, Boolean> retryStatusCodeRanges = new HashMap<>();
        retryStatusCodeRanges.put(501, true);

        Map<Long, ErrorInfo> errors = HttpResponseParser.getErrorsFromResponse(records, responses, retryStatusCodeRanges, createRequestLogStatusCode(), instrumentation);
        Assert.assertEquals(3, errors.size());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(1L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(7L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_5XX_ERROR, errors.get(12L).getErrorType());
        verify(instrumentation, times(3)).logError("Error while pushing message request to http services. Response Code: {}, Response Body: {}", 500, null);
    }

    /**
     * Builds a valid {@link HttpRequestRecord} fixture wrapping an {@code HTTP PUT} to
     * {@code http://dummy.com} with a small JSON entity and an {@code Accept} header, associated with a
     * single message index.
     *
     * @param index the message index (within the batch) the record represents
     * @return the constructed valid record carrying the supplied index
     */
    private HttpRequestRecord createRecord(Integer index) {
        HttpEntityEnclosingRequestBase request = new HttpPut("http://dummy.com");
        request.setEntity(new StringEntity("[{\"key\":\"value1\"},{\"key\":\"value2\"}]", ContentType.APPLICATION_JSON));
        request.setHeader(new BasicHeader("Accept", "text/plain"));
        HttpRequestRecord record = new HttpRequestRecord(null, true, request);
        record.addIndex(index);
        return record;
    }

    /**
     * Produces the request-logging status-code range map used by the fixtures, covering HTTP status
     * codes {@code 401} through {@code 600} via {@link RangeToHashMapConverter}.
     *
     * @return a map whose keys are the status codes for which originating requests should be logged
     */
    private Map<Integer, Boolean> createRequestLogStatusCode() {
        return new RangeToHashMapConverter().convert(null, "401-600");
    }
}
