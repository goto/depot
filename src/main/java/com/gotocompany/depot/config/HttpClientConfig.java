package com.gotocompany.depot.config;


/**
 * Owner configuration interface for the low-level HTTP client shared by Depot's HTTP sink.
 *
 * <p>{@code HttpClientConfig} groups the transport- and authentication-level settings used to build
 * the underlying HTTP client: connection pooling, request timeout, and OAuth2 credentials. It extends
 * {@link SinkConfig} so the common sink and schema settings remain available, and it is in turn
 * extended by {@link HttpSinkConfig}, which adds request-shaping options. All properties live in the
 * {@code SINK_HTTPV2_*} namespace, identifying the second-generation HTTP sink.
 */
public interface HttpClientConfig extends SinkConfig {

    /**
     * Returns the maximum number of connections the HTTP client's connection pool may hold.
     *
     * <p>Bound to the {@code SINK_HTTPV2_MAX_CONNECTIONS} property; defaults to {@code 10}.
     *
     * @return the maximum size of the HTTP connection pool
     */
    @Key("SINK_HTTPV2_MAX_CONNECTIONS")
    @DefaultValue("10")
    Integer getHttpMaxConnections();

    /**
     * Returns the per-request timeout, in milliseconds, applied to outbound HTTP calls.
     *
     * <p>Bound to the {@code SINK_HTTPV2_REQUEST_TIMEOUT_MS} property; defaults to {@code 10000}
     * (10 seconds).
     *
     * @return the HTTP request timeout in milliseconds
     */
    @Key("SINK_HTTPV2_REQUEST_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getHttpRequestTimeoutMs();

    /**
     * Indicates whether OAuth2 authentication is enabled for outbound HTTP requests.
     *
     * <p>When {@code true}, the client obtains and attaches OAuth2 access tokens using the
     * {@code SINK_HTTPV2_OAUTH2_*} credential settings. Bound to the {@code SINK_HTTPV2_OAUTH2_ENABLE}
     * property; defaults to {@code false}.
     *
     * @return {@code true} if OAuth2 authentication should be used, {@code false} otherwise
     */
    @Key("SINK_HTTPV2_OAUTH2_ENABLE")
    @DefaultValue("false")
    Boolean isHttpOAuth2Enable();

    /**
     * Returns the OAuth2 token endpoint URL from which access tokens are requested.
     *
     * <p>Bound to the {@code SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL} property; has no default and is used
     * only when OAuth2 is enabled.
     *
     * @return the OAuth2 access-token endpoint URL, or {@code null} if not configured
     */
    @Key("SINK_HTTPV2_OAUTH2_ACCESS_TOKEN_URL")
    String getHttpOAuth2AccessTokenUrl();

    /**
     * Returns the OAuth2 client identifier presented when requesting access tokens.
     *
     * <p>Bound to the {@code SINK_HTTPV2_OAUTH2_CLIENT_NAME} property; has no default and is used only
     * when OAuth2 is enabled.
     *
     * @return the OAuth2 client name, or {@code null} if not configured
     */
    @Key("SINK_HTTPV2_OAUTH2_CLIENT_NAME")
    String getHttpOAuth2ClientName();

    /**
     * Returns the OAuth2 client secret presented when requesting access tokens.
     *
     * <p>Bound to the {@code SINK_HTTPV2_OAUTH2_CLIENT_SECRET} property; has no default and is used
     * only when OAuth2 is enabled.
     *
     * @return the OAuth2 client secret, or {@code null} if not configured
     */
    @Key("SINK_HTTPV2_OAUTH2_CLIENT_SECRET")
    String getHttpOAuth2ClientSecret();

    /**
     * Returns the OAuth2 scope requested when obtaining access tokens.
     *
     * <p>Bound to the {@code SINK_HTTPV2_OAUTH2_SCOPE} property; has no default and is used only when
     * OAuth2 is enabled.
     *
     * @return the requested OAuth2 scope, or {@code null} if not configured
     */
    @Key("SINK_HTTPV2_OAUTH2_SCOPE")
    String getHttpOAuth2Scope();
}
