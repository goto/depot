package com.gotocompany.depot.http.response;

import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;


/**
 * Unit tests for {@link HttpSinkResponse}, the wrapper around an Apache {@link HttpResponse} that
 * exposes the response code, classifies success versus failure and reads the response body.
 *
 * <p>The Apache {@link HttpResponse}, its {@link StatusLine} and {@link HttpEntity}, and the
 * {@link Instrumentation} facade are all Mockito mocks. The tests drive the status code (including the
 * {@code -1} sentinel produced when no status line is present) to verify the failure flag, the
 * reported response code and the response-body accessor.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class HttpSinkResponseTest {

    /**
     * Mocked Apache HttpResponse wrapped by the response object under test.
     */
    @Mock
    private HttpResponse response;

    /**
     * Mocked entity used when reading the response body.
     */
    @Mock
    private HttpEntity httpEntity;

    /**
     * Mocked status line supplying the HTTP status code.
     */
    @Mock
    private StatusLine statusLine;
    /**
     * Mocked logging and metric facade passed to the response wrapper.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Verifies that a {@code 5xx} status code is treated as a failure.
     *
     * <p>Given a response with status code {@code 500}, when an {@link HttpSinkResponse} is
     * constructed, then {@link HttpSinkResponse#isFail()} returns {@code true}.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldReportWhenFailed() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        Assert.assertTrue(httpSinkResponse.isFail());
    }

    /**
     * Verifies that a {@code 2xx} status code is treated as a success.
     *
     * <p>Given a response with status code {@code 200}, when an {@link HttpSinkResponse} is
     * constructed, then {@link HttpSinkResponse#isFail()} returns {@code false}.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldReportWhenSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        Assert.assertFalse(httpSinkResponse.isFail());
    }

    /**
     * Verifies that the response code is exposed for a successful response.
     *
     * <p>Given a response with status code {@code 200}, when {@link HttpSinkResponse#getResponseCode()}
     * is queried, then it returns {@code 200} and the response is not considered a failure.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldGetResponseCodeIfSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(200);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        int responseCode = httpSinkResponse.getResponseCode();
        Assert.assertFalse(httpSinkResponse.isFail());
        Assert.assertEquals(200, responseCode);
    }

    /**
     * Verifies that the response code is exposed for a failed response.
     *
     * <p>Given a response with status code {@code 500}, when {@link HttpSinkResponse#getResponseCode()}
     * is queried, then it returns {@code 500} and the response is considered a failure.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldGetResponseCodeIfNotSuccess() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        int responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals(500, responseCode);
    }

    /**
     * Verifies that a response without a status line reports the {@code -1} sentinel code and is a
     * failure.
     *
     * <p>Given a response mock whose status line is left unstubbed (and therefore {@code null}), when
     * {@link HttpSinkResponse#getResponseCode()} is queried, then it returns {@code -1} and the
     * response is considered a failure.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldReturnNullResponseCodeIfResponseIsNull() throws IOException {
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        int responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals(-1, responseCode);
    }

    /**
     * Verifies that an explicitly {@code null} status line reports the {@code -1} sentinel code and is
     * a failure.
     *
     * <p>Given a response stubbed to return a {@code null} status line, when
     * {@link HttpSinkResponse#getResponseCode()} is queried, then it returns {@code -1} and the
     * response is considered a failure.</p>
     *
     * @throws IOException never in practice; declared because constructing the wrapper may read the
     *     response
     */
    @Test
    public void shouldReturnNullResponseCodeIfStatusLineIsNull() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(null);
        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);
        int responseCode = httpSinkResponse.getResponseCode();
        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertEquals(-1, responseCode);
    }

    /**
     * Verifies that the response body accessor returns {@code null} when the entity yields no content.
     *
     * <p>Given a failed ({@code 500}) response whose entity has no stubbed content, when
     * {@link HttpSinkResponse#getResponseBody()} is queried, then the response is a failure and the
     * body is {@code null}.</p>
     *
     * @throws IOException never in practice; declared because reading the response body is checked
     */
    @Test
    public void shouldGetResponseBody() throws IOException {
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode()).thenReturn(500);
        Mockito.when(response.getEntity()).thenReturn(httpEntity);

        HttpSinkResponse httpSinkResponse = new HttpSinkResponse(response, instrumentation);

        Assert.assertTrue(httpSinkResponse.isFail());
        Assert.assertNull(httpSinkResponse.getResponseBody());
    }
}
