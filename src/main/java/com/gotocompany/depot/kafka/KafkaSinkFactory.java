package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class KafkaSinkFactory {

    private final KafkaSinkConfig config;
    private final StatsDReporter statsDReporter;
    private final Map<String, String> envVars;

    private ProtoMessageParser sourceMessageParser;
    private ProtoMappingFunction valueMappingFunction;
    private ProtoMappingFunction keyMappingFunction;

    public KafkaSinkFactory(KafkaSinkConfig config, StatsDReporter statsDReporter, Map<String, String> envVars) {
        this.config = config;
        this.statsDReporter = statsDReporter;
        this.envVars = envVars;
    }

    public KafkaSinkFactory(KafkaSinkConfig config, Map<String, String> envVars) {
        this(config, new StatsDReporter(new NoOpStatsDClient()), envVars);
    }

    /**
     * Called once at startup. Builds source + sink stencil clients, compiles CEL expressions.
     * Fails fast if CEL expressions are invalid or stencil is unreachable.
     */
    public void init() {
        log.info("Initialising KafkaSinkFactory: brokers={}, topic={}", config.getSinkKafkaBrokers(), config.getSinkKafkaTopic());

        // Source stencil client — uses the standard SCHEMA_REGISTRY_STENCIL_* vars via SinkConfig
        sourceMessageParser = new ProtoMessageParser(config, statsDReporter, null);

        // Sink stencil client — uses SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_* vars
        StencilClient sinkStencilClient = buildSinkStencilClient();

        // Resolve source and sink descriptors
        Map<String, Descriptors.Descriptor> sourceDescriptors = sourceMessageParser.getDescriptorMap();
        Descriptors.Descriptor sourceMessageDescriptor = getDescriptor(sourceDescriptors, config.getSinkConnectorSchemaProtoMessageClass(), "source message");
        Descriptors.Descriptor sourceKeyDescriptor = getDescriptor(sourceDescriptors, config.getSinkConnectorSchemaProtoKeyClass(), "source key");

        Map<String, Descriptors.Descriptor> sinkDescriptors = sinkStencilClient.getAll();
        Descriptors.Descriptor sinkMessageDescriptor = getDescriptor(sinkDescriptors, config.getSinkKafkaProtoMessage(), "sink message");
        Descriptors.Descriptor sinkKeyDescriptor = getDescriptor(sinkDescriptors, config.getSinkKafkaProtoKey(), "sink key");

        String protoMapping = config.getSinkKafkaProtoMapping();

        // Compile CEL expressions for value and key mappings.
        // Both use the same SINK_KAFKA_PROTO_MAPPING JSON but map to different sink descriptors.
        valueMappingFunction = new ProtoMappingFunction(
                protoMapping,
                config.getSinkConnectorSchemaProtoMessageClass(),
                sourceMessageDescriptor,
                sinkMessageDescriptor);

        keyMappingFunction = new ProtoMappingFunction(
                protoMapping,
                config.getSinkConnectorSchemaProtoKeyClass(),
                sourceKeyDescriptor,
                sinkKeyDescriptor);

        log.info("KafkaSinkFactory initialised successfully");
    }

    /** Creates a new KafkaSink (one per thread as KafkaProducer is not thread-safe). */
    public Sink create() {
        return new KafkaSink(
                sourceMessageParser,
                valueMappingFunction,
                keyMappingFunction,
                new KafkaProducerClient(config, envVars),
                config.getSinkConnectorSchemaProtoMessageClass(),
                config.getSinkConnectorSchemaProtoKeyClass());
    }

    private StencilClient buildSinkStencilClient() {
        StencilConfig stencilConfig = StencilConfig.builder()
                .cacheAutoRefresh(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(config.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                .fetchTimeoutMs(config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                .fetchRetries(config.getSinkKafkaSchemaRegistryStencilFetchRetries())
                .fetchBackoffMinMs(config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                .refreshStrategy(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy())
                .statsDClient(statsDReporter.getClient())
                .build();

        String sinkStencilUrls = config.getSinkKafkaSchemaRegistryStencilUrls();
        if (sinkStencilUrls != null && !sinkStencilUrls.isEmpty()) {
            return StencilClientFactory.getClient(sinkStencilUrls, stencilConfig);
        }
        // Fall back to same stencil server as source if not configured separately
        return StencilClientFactory.getClient(config.getSchemaRegistryStencilUrls(), stencilConfig);
    }

    private static Descriptors.Descriptor getDescriptor(
            Map<String, Descriptors.Descriptor> descriptors, String protoClass, String role) {
        Descriptors.Descriptor descriptor = descriptors.get(protoClass);
        if (descriptor == null) {
            throw new IllegalArgumentException(
                    "Proto descriptor not found for " + role + " class '" + protoClass + "'. "
                    + "Check that the stencil URL is correct and the proto is published.");
        }
        return descriptor;
    }
}
