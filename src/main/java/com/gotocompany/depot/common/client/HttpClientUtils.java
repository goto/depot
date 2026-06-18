package com.gotocompany.depot.common.client;

import com.gotocompany.depot.common.client.auth.OAuth2Credential;
import com.gotocompany.depot.config.HttpClientConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * Factory helpers for building Apache {@link CloseableHttpClient} instances used by Depot's HTTP
 * sink.
 *
 * <p>{@code HttpClientUtils} centralizes the construction of a connection-pooled HTTP client whose
 * timeouts and pool size are taken from an {@link HttpClientConfig}. When OAuth2 is enabled in
 * configuration, the resulting client is additionally wired with an {@link OAuth2Credential}
 * interceptor that attaches and refreshes bearer tokens automatically.
 */
public class HttpClientUtils {

    /**
     * Builds a connection-pooled Apache HTTP client configured from the supplied settings.
     *
     * <p>The socket, connection-request and connect timeouts are all set to the configured request
     * timeout, and the connection pool's total and per-route maxima are set to the configured maximum
     * connection count. When {@link HttpClientConfig#isHttpOAuth2Enable()} is {@code true}, an
     * {@link OAuth2Credential} built from the configured client credentials, scope and token endpoint
     * is registered as request and response interceptors so outbound requests carry a valid bearer
     * token.
     *
     * @param config         the HTTP client configuration supplying timeouts, pool size and OAuth2
     *                       settings
     * @param statsDReporter the reporter used to instrument the OAuth2 credential when OAuth2 is
     *                       enabled
     * @return a newly built, ready-to-use {@link CloseableHttpClient}
     */
    public static CloseableHttpClient newHttpClient(HttpClientConfig config, StatsDReporter statsDReporter) {
        Integer maxHttpConnections = config.getHttpMaxConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(config.getHttpRequestTimeoutMs())
                .setConnectionRequestTimeout(config.getHttpRequestTimeoutMs())
                .setConnectTimeout(config.getHttpRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        if (config.isHttpOAuth2Enable()) {
            OAuth2Credential oauth2 = new OAuth2Credential(
                    new Instrumentation(statsDReporter, OAuth2Credential.class),
                    config.getHttpOAuth2ClientName(),
                    config.getHttpOAuth2ClientSecret(),
                    config.getHttpOAuth2Scope(),
                    config.getHttpOAuth2AccessTokenUrl());
            builder = oauth2.initialize(builder);
        }

        return builder.build();
    }
}
