package com.gotocompany.depot.http.enums;

/**
 * Enumerates the batching strategy the HTTP sink applies when turning a batch of consumed messages
 * into outbound HTTP requests.
 *
 * <p>The value is resolved from the {@code SINK_HTTPV2_REQUEST_MODE} configuration key and selects
 * which {@link com.gotocompany.depot.http.request.Request} implementation
 * {@link com.gotocompany.depot.http.request.RequestFactory} instantiates. It dictates the
 * one-message-per-request versus many-messages-per-request trade-off and, in turn, which
 * per-message templating features (dynamic headers, query parameters and URLs) are permitted.</p>
 *
 * @see com.gotocompany.depot.http.request.RequestFactory
 * @see com.gotocompany.depot.http.request.SingleRequest
 * @see com.gotocompany.depot.http.request.BatchRequest
 */
public enum HttpRequestType {
    /**
     * Emits one HTTP request per message.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.SingleRequest}. Because each request is
     * built from exactly one message, headers, query parameters and the service URL may all contain
     * per-message templates that are resolved from that message's parsed key or value.</p>
     */
    SINGLE,
    /**
     * Combines all valid messages in a batch into a single HTTP request.
     *
     * <p>Backed by {@link com.gotocompany.depot.http.request.BatchRequest}. Since one request covers
     * many messages, the headers, query parameters and service URL must be constant: per-message
     * templates in those components are rejected, while the request body aggregates the serialized
     * payloads of every valid message.</p>
     */
    BATCH
}
