package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.StatsDClient;

public class KafkaSinkStencilUtils {

    public static StencilConfig getSinkStencilConfig(KafkaSinkConfig config, StatsDClient statsDClient) {
        return StencilConfig.builder()
                .cacheAutoRefresh(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(config.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(config.getSinkKafkaSchemaRegistryStencilFetchHeaders())
                .fetchBackoffMinMs(config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(config.getSinkKafkaSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy())
                .build();
    }

    public static StencilConfig getSourceStencilConfig(KafkaSinkConfig config, StatsDClient statsDClient) {
        return StencilConfig.builder()
                .cacheAutoRefresh(config.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(config.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(config.getSchemaRegistryStencilFetchHeaders())
                .fetchBackoffMinMs(config.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(config.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(config.getSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(config.getSchemaRegistryStencilRefreshStrategy())
                .build();
    }
}
