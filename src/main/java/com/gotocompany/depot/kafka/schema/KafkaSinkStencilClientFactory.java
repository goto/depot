package com.gotocompany.depot.kafka.schema;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.stencil.SchemaUpdateListener;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.StatsDClient;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory for the sink Stencil client used to fetch the output key and value proto descriptors.
 *
 * <p>When the sink Stencil registry is disabled the descriptors are loaded from the classpath, otherwise a
 * URL backed Stencil client is created from the sink Stencil configuration.
 */
public final class KafkaSinkStencilClientFactory {

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    /**
     * Prevents instantiation of this utility class.
     */
    private KafkaSinkStencilClientFactory() {
    }

    /**
     * Creates the sink Stencil client based on whether the sink Stencil registry is enabled.
     *
     * @param sinkConfig     the Kafka sink configuration
     * @param statsDClient   the StatsD client used by the Stencil client
     * @param updateListener the listener notified when the sink schemas are refreshed
     * @return a URL backed Stencil client when enabled, otherwise a classpath backed client
     */
    public static StencilClient create(KafkaSinkConfig sinkConfig, StatsDClient statsDClient, SchemaUpdateListener updateListener) {
        if (sinkConfig.isSinkKafkaSchemaRegistryStencilEnable()) {
            return StencilClientFactory.getClient(
                    sinkConfig.getSinkKafkaSchemaRegistryStencilUrls(),
                    getStencilConfig(sinkConfig, statsDClient, updateListener));
        }
        return StencilClientFactory.getClient();
    }

    /**
     * Builds the Stencil configuration from the sink Stencil settings.
     *
     * @param sinkConfig     the Kafka sink configuration
     * @param statsDClient   the StatsD client used by the Stencil client
     * @param updateListener the listener notified when the sink schemas are refreshed
     * @return the Stencil configuration
     */
    public static StencilConfig getStencilConfig(KafkaSinkConfig sinkConfig, StatsDClient statsDClient, SchemaUpdateListener updateListener) {
        return StencilConfig.builder()
                .cacheAutoRefresh(sinkConfig.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(sinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(getFetchHeaders(sinkConfig))
                .fetchBackoffMinMs(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(sinkConfig.getSinkKafkaSchemaRegistryStencilRefreshStrategy())
                .updateListener(updateListener)
                .build();
    }

    /**
     * Builds the fetch headers, adding an authorization header when a bearer token is configured.
     *
     * @param sinkConfig the Kafka sink configuration
     * @return the list of fetch headers, empty when no bearer token is configured
     */
    private static List<Header> getFetchHeaders(KafkaSinkConfig sinkConfig) {
        List<Header> headers = new ArrayList<>();
        String bearerToken = sinkConfig.getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken();
        if (bearerToken != null) {
            headers.add(new BasicHeader(AUTHORIZATION_HEADER, BEARER_PREFIX + bearerToken));
        }
        return headers;
    }
}
