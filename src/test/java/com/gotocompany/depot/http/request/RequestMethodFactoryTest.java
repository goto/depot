package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;

/**
 * Unit tests for {@link RequestMethodFactory}, the factory that maps a {@link HttpRequestMethodType}
 * to the corresponding Apache {@link HttpEntityEnclosingRequestBase} implementation.
 *
 * <p>Each test asks the factory to build a request for a specific method and asserts that the produced
 * request reports the matching HTTP method name. Only the method is under test, so the URI value is
 * immaterial to the assertions.</p>
 */
public class RequestMethodFactoryTest {

    /**
     * Placeholder request URI passed to the factory; its value is irrelevant because the tests assert
     * only on the produced HTTP method.
     */
    @Mock
    private URI uri;

    /**
     * Verifies that the factory builds a {@code PUT} request for {@link HttpRequestMethodType#PUT}.
     *
     * <p>When {@link RequestMethodFactory#create(URI, HttpRequestMethodType)} is called with
     * {@code PUT}, then the produced request's method is {@code "PUT"}.</p>
     */
    @Test
    public void shouldReturnPutRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.PUT);
        Assert.assertEquals("PUT", request.getMethod());

    }

    /**
     * Verifies that the factory builds a {@code POST} request for {@link HttpRequestMethodType#POST}.
     *
     * <p>When {@link RequestMethodFactory#create(URI, HttpRequestMethodType)} is called with
     * {@code POST}, then the produced request's method is {@code "POST"}.</p>
     */
    @Test
    public void shouldReturnPostRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.POST);
        Assert.assertEquals("POST", request.getMethod());
    }

    /**
     * Verifies that the factory builds a {@code PATCH} request for {@link HttpRequestMethodType#PATCH}.
     *
     * <p>When {@link RequestMethodFactory#create(URI, HttpRequestMethodType)} is called with
     * {@code PATCH}, then the produced request's method is {@code "PATCH"}.</p>
     */
    @Test
    public void shouldReturnPatchRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.PATCH);
        Assert.assertEquals("PATCH", request.getMethod());
    }

    /**
     * Verifies that the factory builds a {@code DELETE} request for
     * {@link HttpRequestMethodType#DELETE}.
     *
     * <p>When {@link RequestMethodFactory#create(URI, HttpRequestMethodType)} is called with
     * {@code DELETE}, then the produced request's method is {@code "DELETE"}.</p>
     */
    @Test
    public void shouldReturnDeleteRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.DELETE);
        Assert.assertEquals("DELETE", request.getMethod());
    }
}
