package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;

public class KafkaSinkFactory {

    private final KafkaSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private StencilClient sinkStencilClient;

    public KafkaSinkFactory(KafkaSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public KafkaSinkFactory(KafkaSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    public void init() {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSinkFactory.class);
        instrumentation.logInfo("Initializing Kafka Sink Factory");

        String sinkStencilUrls = sinkConfig.getSinkKafkaSchemaRegistryStencilUrls();
        if (sinkStencilUrls != null && !sinkStencilUrls.isEmpty()) {
            StencilConfig sinkStencilConfig = StencilConfig.builder()
                    .cacheAutoRefresh(sinkConfig.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                    .cacheTtlMs(sinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                    .fetchBackoffMinMs(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                    .fetchRetries(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries())
                    .fetchTimeoutMs(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                    .refreshStrategy(sinkConfig.getSinkKafkaSchemaRegistryStencilRefreshStrategy())
                    .statsDClient(statsDReporter.getClient())
                    .build();
            sinkStencilClient = StencilClientFactory.getClient(sinkStencilUrls, sinkStencilConfig);
            instrumentation.logInfo("Sink Stencil client initialized with URLs: {}", sinkStencilUrls);
        } else {
            sinkStencilClient = StencilClientFactory.getClient();
            instrumentation.logInfo("Sink Stencil client initialized without schema registry");
        }

        instrumentation.logInfo("\n\tKafka sink config:"
                        + "\n\tbrokers = {}"
                        + "\n\ttopic = {}"
                        + "\n\tproto.message = {}"
                        + "\n\tproto.key = {}"
                        + "\n\tproduce.large.message.enable = {}",
                sinkConfig.getSinkKafkaBrokers(),
                sinkConfig.getSinkKafkaTopic(),
                sinkConfig.getSinkKafkaProtoMessage(),
                sinkConfig.getSinkKafkaProtoKey(),
                sinkConfig.isSinkKafkaProduceLargeMessageEnabled());
    }

    public Sink create() {
        return new KafkaSink(
                sinkConfig,
                new Instrumentation(statsDReporter, KafkaSink.class));
    }

    public StencilClient getSinkStencilClient() {
        return sinkStencilClient;
    }
}
