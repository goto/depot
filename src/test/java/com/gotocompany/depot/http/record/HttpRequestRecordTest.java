package com.gotocompany.depot.http.record;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HttpRequestRecord}, the unit of work that pairs a built HTTP request with the
 * message indexes it represents.
 *
 * <p>The tests verify the record's accessors ({@link HttpRequestRecord#isValid()},
 * {@link HttpRequestRecord#getErrorInfo()}), its iteration over the covered message indexes, the
 * {@link HttpRequestRecord#send(HttpClient, Instrumentation)} call that executes the wrapped request
 * and wraps the result in an {@link HttpSinkResponse}, and {@link HttpRequestRecord#getRequestBody()}
 * which serialises the request entity. The Apache HttpClient request, client, response, entity and
 * status line are all Mockito mocks so behaviour can be driven without real I/O.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpRequestRecordTest {

    /**
     * Mocked HTTP request wrapped by the record under test.
     */
    @Mock
    private HttpEntityEnclosingRequestBase httpRequest;

    /**
     * Mocked client used to execute the wrapped request during send tests.
     */
    @Mock
    private HttpClient httpClient;

    /**
     * Mocked response returned by the client's execute call.
     */
    @Mock
    private HttpResponse httpResponse;

    /**
     * Mocked response/request entity used to drive body reads.
     */
    @Mock
    private HttpEntity httpEntity;

    /**
     * Mocked status line supplying the HTTP status code that determines success or failure.
     */
    @Mock
    private StatusLine statusLine;
    /**
     * Mocked logging and metric facade passed to the produced {@link HttpSinkResponse}.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Verifies that a record created for a single message exposes exactly one index through its
     * iterator.
     *
     * <p>Given a record associated with index {@code 0}, when its iterator is traversed, then the
     * first call to {@code hasNext()} is {@code true}, {@code next()} yields {@code 0}, and a
     * subsequent {@code hasNext()} is {@code false}.</p>
     */
    @Test
    public void shouldExactlyGetOneRecordIndex() {
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        Iterator<Integer> indexIterator = httpRequestRecord.iterator();
        assertTrue(indexIterator.hasNext());
        assertEquals(0, (int) indexIterator.next());
        assertFalse(indexIterator.hasNext());
    }

    /**
     * Verifies that a record returns the {@link ErrorInfo} it was constructed with.
     *
     * <p>Given a record built with a {@link ErrorType#DESERIALIZATION_ERROR} error, when
     * {@link HttpRequestRecord#getErrorInfo()} is called, then it returns that same error instance.</p>
     */
    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR);
        HttpRequestRecord httpRequestRecord = createRecord(errorInfo, true);
        Assert.assertEquals(errorInfo, httpRequestRecord.getErrorInfo());
    }

    /**
     * Verifies that a record created with the valid flag set reports itself as valid.
     *
     * <p>Given a record built with {@code isValid = true}, when {@link HttpRequestRecord#isValid()}
     * is queried, then it returns {@code true}.</p>
     */
    @Test
    public void shouldGetValidRecord() {
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertTrue(httpRequestRecord.isValid());
    }

    /**
     * Verifies that a record created with the valid flag cleared reports itself as invalid.
     *
     * <p>Given a record built with an error and {@code isValid = false}, when
     * {@link HttpRequestRecord#isValid()} is queried, then it returns {@code false}.</p>
     */
    @Test
    public void shouldGetInvalidRecord() {
        ErrorInfo errorInfo = new ErrorInfo(new DeserializerException("Deserialization Error"), ErrorType.DESERIALIZATION_ERROR);
        HttpRequestRecord httpRequestRecord = createRecord(errorInfo, false);
        assertFalse(httpRequestRecord.isValid());
    }

    /**
     * Verifies that sending a record whose execution returns a {@code 200} produces a non-failing
     * response.
     *
     * <p>Given a client stubbed to return a response with status code {@code 200}, when
     * {@link HttpRequestRecord#send(HttpClient, Instrumentation)} is called, then the resulting
     * {@link HttpSinkResponse} reports {@code isFail() == false}.</p>
     *
     * @throws IOException never in practice; declared because the send and execute methods are checked
     */
    @Test
    public void shouldSendHttpRequestWithSuccessResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(200);
        HttpRequestRecord requestRecord = createRecord(null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient, instrumentation);
        assertFalse(sinkResponse.isFail());
    }

    /**
     * Verifies that sending a record whose execution returns a {@code 500} produces a failing
     * response.
     *
     * <p>Given a client stubbed to return a response with status code {@code 500} and an entity, when
     * {@link HttpRequestRecord#send(HttpClient, Instrumentation)} is called, then the resulting
     * {@link HttpSinkResponse} reports {@code isFail() == true}.</p>
     *
     * @throws IOException never in practice; declared because the send and execute methods are checked
     */
    @Test
    public void shouldSendHttpRequestWithFailedResponse() throws IOException {
        when(httpClient.execute(httpRequest)).thenReturn(httpResponse);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(500);
        when(httpResponse.getEntity()).thenReturn(httpEntity);

        HttpRequestRecord requestRecord = createRecord(null, true);
        HttpSinkResponse sinkResponse = requestRecord.send(httpClient, instrumentation);
        assertTrue(sinkResponse.isFail());
    }

    /**
     * Verifies that the record serialises its request entity into the expected body string.
     *
     * <p>Given a request whose entity content is a known JSON string, when
     * {@link HttpRequestRecord#getRequestBody()} is called, then it returns that exact string.</p>
     *
     * @throws IOException never in practice; declared because reading the entity is a checked operation
     */
    @Test
    public void shouldGetRequestBody() throws IOException {
        String body = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        when(httpRequest.getEntity()).thenReturn(httpEntity);
        when(httpEntity.getContent()).thenReturn(new StringInputStream(body));
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertEquals(body, httpRequestRecord.getRequestBody());
    }

    /**
     * Verifies that reading the body of a request whose entity yields no content returns {@code null}.
     *
     * <p>Given a request whose entity has no stubbed content, when
     * {@link HttpRequestRecord#getRequestBody()} is called, then it returns {@code null}.</p>
     *
     * @throws IOException never in practice; declared because reading the entity is a checked operation
     */
    @Test
    public void shouldGetNullIfRequestIsNull() throws IOException {
        when(httpRequest.getEntity()).thenReturn(httpEntity);
        HttpRequestRecord httpRequestRecord = createRecord(null, true);
        assertNull(httpRequestRecord.getRequestBody());
    }

    /**
     * Builds a {@link HttpRequestRecord} around the mocked request and associates it with index
     * {@code 0}.
     *
     * @param errorInfo the error to attach, or {@code null} for a record without a construction error
     * @param isValid {@code true} to mark the record as valid; {@code false} otherwise
     * @return the constructed record carrying the supplied error and validity flag
     */
    private HttpRequestRecord createRecord(ErrorInfo errorInfo, boolean isValid) {
        HttpRequestRecord record = new HttpRequestRecord(errorInfo, isValid, httpRequest);
        record.addIndex(0);
        return record;
    }
}
