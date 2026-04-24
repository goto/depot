package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;

import java.util.Map;

public class KafkaSinkFactory {

    private final KafkaSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private final Map<String, String> envVars;
    private StencilClient sinkStencilClient;
    private StencilClient sourceStencilClient;
    private KafkaProtoMappingEngine mappingEngine;

    public KafkaSinkFactory(KafkaSinkConfig sinkConfig, StatsDReporter statsDReporter, Map<String, String> envVars) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
        this.envVars = envVars;
    }

    public KafkaSinkFactory(KafkaSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this(sinkConfig, statsDReporter, null);
    }

    public KafkaSinkFactory(KafkaSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
    }

    public void init() {
        Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSinkFactory.class);
        instrumentation.logInfo("Initializing Kafka Sink Factory");

        // Initialize source stencil client (for parsing incoming messages)
        this.sourceStencilClient = StencilClientFactory.getClient();

        // Initialize sink stencil client (for output proto schema)
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

        // Initialize the CEL-based proto mapping engine (compiles all expressions at startup — fail-fast)
        this.mappingEngine = new KafkaProtoMappingEngine(sinkConfig, sourceStencilClient, sinkStencilClient, instrumentation);

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
        Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSink.class);
        KafkaSinkProducer producer = new KafkaSinkProducer(sinkConfig, envVars, instrumentation);
        return new KafkaSink(
                sinkConfig,
                mappingEngine,
                producer,
                sourceStencilClient,
                instrumentation);
    }

    public StencilClient getSinkStencilClient() {
        return sinkStencilClient;
    }
}
