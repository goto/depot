package com.gotocompany.depot.http.enums;

/**
 * Enumerates which portion of a record supplies the field values used to resolve templated HTTP
 * headers and query parameters.
 *
 * <p>The header source is resolved from {@code SINK_HTTPV2_HEADERS_PARAMETER_SOURCE} and the query
 * parameter source from {@code SINK_HTTPV2_QUERY_PARAMETER_SOURCE}. The selected constant tells the
 * {@link com.gotocompany.depot.http.request.builder.HeaderBuilder} and
 * {@link com.gotocompany.depot.http.request.builder.QueryParamBuilder} whether to evaluate their
 * templates against the parsed key or the parsed value of each message.</p>
 *
 * @see com.gotocompany.depot.http.request.builder.HeaderBuilder
 * @see com.gotocompany.depot.http.request.builder.QueryParamBuilder
 */
public enum HttpParameterSourceType {
    /**
     * Resolves template values from the parsed log key of the message.
     */
    KEY,
    /**
     * Resolves template values from the parsed log message (value) of the message.
     */
    MESSAGE,
}
