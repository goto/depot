package com.gotocompany.depot.http.request;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import com.gotocompany.depot.http.enums.HttpRequestType;
import com.gotocompany.depot.message.MessageParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link RequestFactory}, the factory that selects the appropriate {@link Request}
 * implementation based on the configured {@link HttpRequestType}.
 *
 * <p>The {@link HttpSinkConfig} and {@link MessageParser} are Mockito mocks. Each test stubs the
 * request type and asserts on the concrete runtime type returned by
 * {@link RequestFactory#create(HttpSinkConfig, MessageParser)}.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RequestFactoryTest {

    /**
     * Mocked configuration whose request type and body type drive the factory's selection.
     */
    @Mock
    private HttpSinkConfig sinkConfig;
    /**
     * Mocked message parser forwarded to the created request.
     */
    @Mock
    private MessageParser messageParser;

    /**
     * Verifies that a {@link HttpRequestType#SINGLE} configuration yields a {@link SingleRequest}.
     *
     * <p>Given a configuration whose request type is {@code SINGLE} and body type is
     * {@link HttpRequestBodyType#RAW}, when {@link RequestFactory#create(HttpSinkConfig, MessageParser)}
     * is called, then the result is an instance of {@link SingleRequest}.</p>
     *
     * @throws InvalidTemplateException never in practice; declared because URI template parsing during
     *     request creation is checked
     */
    @Test
    public void shouldReturnSingleRequest() throws InvalidTemplateException {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.SINGLE);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(sinkConfig, messageParser);
        Assert.assertTrue(request instanceof SingleRequest);
    }

    /**
     * Verifies that a {@link HttpRequestType#BATCH} configuration yields a {@link BatchRequest}.
     *
     * <p>Given a configuration whose request type is {@code BATCH} and body type is
     * {@link HttpRequestBodyType#RAW}, when {@link RequestFactory#create(HttpSinkConfig, MessageParser)}
     * is called, then the result is an instance of {@link BatchRequest}.</p>
     *
     * @throws InvalidTemplateException never in practice; declared because URI template parsing during
     *     request creation is checked
     */
    @Test
    public void shouldReturnBatchRequest() throws InvalidTemplateException {
        Mockito.when(sinkConfig.getSinkHttpServiceUrl()).thenReturn("http://dummy.com");
        Mockito.when(sinkConfig.getRequestType()).thenReturn(HttpRequestType.BATCH);
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        Request request = RequestFactory.create(sinkConfig, messageParser);
        Assert.assertTrue(request instanceof BatchRequest);
    }
}
