package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.stencil.SchemaUpdateListener;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.StatsDClient;

/**
 * Helper that translates Depot sink configuration into a Stencil configuration object.
 *
 * <p>Stencil is the schema registry client Depot uses to resolve Protobuf descriptors. This helper
 * builds a fully populated {@link com.gotocompany.stencil.config.StencilConfig} from the
 * schema-registry-related settings on a {@link SinkConfig} — cache refresh and TTL, fetch headers,
 * back-off, retries and timeout, and refresh strategy — and attaches the StatsD client used for
 * Stencil's own metrics, optionally wiring a schema-update listener.</p>
 */
public class StencilUtils {
    /**
     * Builds a Stencil configuration from sink configuration, with a schema-update listener attached.
     *
     * <p>Copies the schema-registry settings from {@code sinkConfig} into a
     * {@link com.gotocompany.stencil.config.StencilConfig}: automatic cache refresh, cache TTL, fetch
     * headers, minimum fetch back-off, fetch retries, fetch timeout and refresh strategy. The supplied
     * {@link SchemaUpdateListener} is registered so the caller is notified when schemas change, and
     * {@code statsDClient} is wired for Stencil's metrics.</p>
     *
     * @param sinkConfig the sink configuration supplying the schema-registry settings
     * @param statsDClient the StatsD client Stencil uses to emit its metrics
     * @param schemaUpdateListener the listener to notify on schema updates, or {@code null} for none
     * @return a fully populated {@link com.gotocompany.stencil.config.StencilConfig}
     */
    public static StencilConfig getStencilConfig(
            SinkConfig sinkConfig,
            StatsDClient statsDClient,
            SchemaUpdateListener schemaUpdateListener) {
        return StencilConfig.builder()
                .cacheAutoRefresh(sinkConfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(sinkConfig.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(sinkConfig.getSchemaRegistryStencilFetchHeaders())
                .fetchBackoffMinMs(sinkConfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(sinkConfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(sinkConfig.getSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(sinkConfig.getSchemaRegistryStencilRefreshStrategy())
                .updateListener(schemaUpdateListener)
                .build();
    }

    /**
     * Builds a Stencil configuration from sink configuration without a schema-update listener.
     *
     * <p>Convenience overload that delegates to
     * {@link #getStencilConfig(SinkConfig, StatsDClient, SchemaUpdateListener)} with a {@code null}
     * listener.</p>
     *
     * @param config the sink configuration supplying the schema-registry settings
     * @param statsDClient the StatsD client Stencil uses to emit its metrics
     * @return a fully populated {@link com.gotocompany.stencil.config.StencilConfig} with no listener
     *     registered
     */
    public static StencilConfig getStencilConfig(SinkConfig config, StatsDClient statsDClient) {
        return getStencilConfig(config, statsDClient, null);
    }
}
