package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;

/**
 * Factory that selects a {@link RequestBody} implementation based on the configured body type.
 *
 * <p>This is the single place mapping {@link HttpRequestBodyType} onto a concrete serializer, used by
 * both {@link com.gotocompany.depot.http.request.SingleRequest} and
 * {@link com.gotocompany.depot.http.request.BatchRequest} when they assemble payloads.</p>
 *
 * @see RequestBody
 * @see HttpRequestBodyType
 */
public class RequestBodyFactory {

    /**
     * Creates the request body serializer matching the configured body type.
     *
     * <p>{@link HttpRequestBodyType#JSON}, {@link HttpRequestBodyType#MESSAGE} and
     * {@link HttpRequestBodyType#TEMPLATIZED_JSON} map to {@link JsonBody}, {@link MessageBody} and
     * {@link TemplatizedJsonBody} respectively; any other value (notably
     * {@link HttpRequestBodyType#RAW}) maps to {@link RawBody}.</p>
     *
     * @param config the HTTP sink configuration supplying the body type and related settings
     * @return a {@link RequestBody} implementation for the configured body type
     */
    public static RequestBody create(HttpSinkConfig config) {
        HttpRequestBodyType bodyType = config.getRequestBodyType();
        switch (bodyType) {
            case JSON:
                return new JsonBody(config);
            case MESSAGE:
                return new MessageBody(config);
            case TEMPLATIZED_JSON:
                return new TemplatizedJsonBody(config);
            default:
                return new RawBody(config);
        }
    }
}
