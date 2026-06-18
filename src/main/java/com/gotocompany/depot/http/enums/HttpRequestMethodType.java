package com.gotocompany.depot.http.enums;

/**
 * Enumerates the HTTP verbs the sink can use when dispatching requests to the downstream service.
 *
 * <p>The value is resolved from the {@code SINK_HTTPV2_REQUEST_METHOD} configuration key and is
 * mapped to a concrete Apache HttpComponents request object by
 * {@link com.gotocompany.depot.http.request.RequestMethodFactory}. Every supported verb is realized
 * as an entity-enclosing request so that a body may be attached, including {@code DELETE} which is
 * served by the custom {@link com.gotocompany.depot.http.request.method.HttpDeleteWithBody}.</p>
 *
 * @see com.gotocompany.depot.http.request.RequestMethodFactory
 */
public enum HttpRequestMethodType {
    /**
     * The HTTP {@code PUT} method; mapped to {@link org.apache.http.client.methods.HttpPut} and used
     * as the factory's default when no other verb matches.
     */
    PUT,
    /**
     * The HTTP {@code POST} method; mapped to {@link org.apache.http.client.methods.HttpPost}.
     */
    POST,
    /**
     * The HTTP {@code PATCH} method; mapped to {@link org.apache.http.client.methods.HttpPatch}.
     */
    PATCH,
    /**
     * The HTTP {@code DELETE} method; mapped to the body-capable
     * {@link com.gotocompany.depot.http.request.method.HttpDeleteWithBody}. Whether a body is
     * actually attached to a delete request is governed by the
     * {@code SINK_HTTPV2_DELETE_BODY_ENABLE} configuration flag.
     */
    DELETE
}
