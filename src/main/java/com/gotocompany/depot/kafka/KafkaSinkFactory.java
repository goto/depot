package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import lombok.extern.slf4j.Slf4j;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

@Slf4j
public class KafkaSinkFactory {

    private final KafkaSinkConfig config;
    private final StatsDReporter statsDReporter;
    private final Map<String, String> envVars;

    private StencilClient sourceStencilClient;
    private StencilClient sinkStencilClient;
    private ProtoMapper valueMapper;
    private ProtoMapper keyMapper;
    private KafkaMessageParser messageParser;
    private KafkaMessageSerializer serializer;

    public KafkaSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(ConfigFactory.create(KafkaSinkConfig.class, env), statsDReporter, env);
    }

    public KafkaSinkFactory(KafkaSinkConfig config, StatsDReporter statsDReporter, Map<String, String> envVars) {
        this.config = config;
        this.statsDReporter = statsDReporter;
        this.envVars = envVars;
    }

    public KafkaSinkFactory(KafkaSinkConfig config, StatsDReporter statsDReporter) {
        this(config, statsDReporter, null);
    }

    public KafkaSinkFactory(KafkaSinkConfig config) {
        this(config, new StatsDReporter(new NoOpStatsDClient()), null);
    }

    public void init() {
        try {
            Instrumentation instrumentation = new Instrumentation(statsDReporter, KafkaSinkFactory.class);
            instrumentation.logInfo("Initializing Kafka Sink Factory");

            StencilConfig sourceStencilConfig = KafkaSinkStencilUtils.getSourceStencilConfig(
                    config, statsDReporter.getClient());

            if (config.isSchemaRegistryStencilEnable()) {
                sourceStencilClient = StencilClientFactory.getClient(
                        config.getSchemaRegistryStencilUrls(), sourceStencilConfig);
            } else {
                sourceStencilClient = StencilClientFactory.getClient();
            }

            StencilConfig sinkStencilConfig = KafkaSinkStencilUtils.getSinkStencilConfig(
                    config, statsDReporter.getClient());
            sinkStencilClient = StencilClientFactory.getClient(
                    config.getSinkKafkaSchemaRegistryStencilUrls(), sinkStencilConfig);

            String sourceProtoClass = config.getInputSchemaProtoClass();
            String sinkProtoMessage = config.getSinkKafkaProtoMessage();
            String sinkProtoKey = config.getSinkKafkaProtoKey();
            String mappingJson = config.getSinkKafkaProtoMapping();

            Descriptors.Descriptor sourceDescriptor = sourceStencilClient.get(sourceProtoClass);
            Descriptors.Descriptor sinkValueDescriptor = sinkStencilClient.get(sinkProtoMessage);

            valueMapper = new ProtoMapper(sourceDescriptor, sinkValueDescriptor, mappingJson);
            instrumentation.logInfo("Value proto mapper initialized: {} -> {}",
                    sourceProtoClass, sinkProtoMessage);

            if (sinkProtoKey != null && !sinkProtoKey.isEmpty()) {
                Descriptors.Descriptor sinkKeyDescriptor = sinkStencilClient.get(sinkProtoKey);
                keyMapper = new ProtoMapper(sourceDescriptor, sinkKeyDescriptor, mappingJson);
                instrumentation.logInfo("Key proto mapper initialized: {} -> {}",
                        sourceProtoClass, sinkProtoKey);
            }

            messageParser = new KafkaMessageParser(sourceStencilClient, sourceProtoClass);
            serializer = new KafkaMessageSerializer();

            instrumentation.logInfo("Kafka Sink Factory initialized successfully");
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception occurred while creating Kafka sink", e);
        }
    }

    public Sink create() {
        KafkaProducerWrapper producerWrapper = new KafkaProducerWrapper(config, envVars);
        return new KafkaSink(messageParser, valueMapper, keyMapper, serializer, producerWrapper);
    }
}
