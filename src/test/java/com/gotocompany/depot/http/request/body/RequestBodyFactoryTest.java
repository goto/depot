package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for {@link RequestBodyFactory}, the factory that selects a {@link RequestBody}
 * implementation based on the configured {@link HttpRequestBodyType}.
 *
 * <p>The {@link HttpSinkConfig} is a Mockito mock. Each test stubs the body type (and, for the
 * templated case, the JSON body template) and asserts on the concrete runtime type returned by
 * {@link RequestBodyFactory#create(HttpSinkConfig)}.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RequestBodyFactoryTest {

    /**
     * Mocked configuration whose body type drives the factory's selection.
     */
    @Mock
    private HttpSinkConfig sinkConfig;

    /**
     * Verifies that a {@link HttpRequestBodyType#RAW} configuration yields a {@link RawBody}.
     *
     * <p>Given the configured body type is {@code RAW}, when
     * {@link RequestBodyFactory#create(HttpSinkConfig)} is called, then the result is an instance of
     * {@link RawBody}.</p>
     */
    @Test
    public void shouldReturnRawBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.RAW);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof RawBody);
    }

    /**
     * Verifies that a {@link HttpRequestBodyType#JSON} configuration yields a {@link JsonBody}.
     *
     * <p>Given the configured body type is {@code JSON}, when
     * {@link RequestBodyFactory#create(HttpSinkConfig)} is called, then the result is an instance of
     * {@link JsonBody}.</p>
     */
    @Test
    public void shouldReturnJsonBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.JSON);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof JsonBody);
    }

    /**
     * Verifies that a {@link HttpRequestBodyType#MESSAGE} configuration yields a {@link MessageBody}.
     *
     * <p>Given the configured body type is {@code MESSAGE}, when
     * {@link RequestBodyFactory#create(HttpSinkConfig)} is called, then the result is an instance of
     * {@link MessageBody}.</p>
     */
    @Test
    public void shouldReturnMessageBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.MESSAGE);
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof MessageBody);
    }

    /**
     * Verifies that a {@link HttpRequestBodyType#TEMPLATIZED_JSON} configuration yields a
     * {@link TemplatizedJsonBody}.
     *
     * <p>Given the configured body type is {@code TEMPLATIZED_JSON} and a non-empty JSON body template
     * ({@code {}}) is provided, when {@link RequestBodyFactory#create(HttpSinkConfig)} is called, then
     * the result is an instance of {@link TemplatizedJsonBody}.</p>
     */
    @Test
    public void shouldReturnTemplatizedJsonBodyType() {
        Mockito.when(sinkConfig.getRequestBodyType()).thenReturn(HttpRequestBodyType.TEMPLATIZED_JSON);
        Mockito.when(sinkConfig.getSinkHttpJsonBodyTemplate()).thenReturn("{}");
        RequestBody requestBody = RequestBodyFactory.create(sinkConfig);
        Assert.assertTrue(requestBody instanceof TemplatizedJsonBody);
    }
}
