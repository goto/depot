package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.kafka.mapping.ProtoMappingParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

public class KafkaSinkFactory {

    private static final int MAX_REQUEST_SIZE = 20971520;

    private final KafkaSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;

    private StencilClient sourceStencilClient;
    private StencilClient sinkStencilClient;
    private ProtoMappingFunction mappingFunction;
    private Descriptors.Descriptor sourceMessageDescriptor;
    private Descriptors.Descriptor sourceKeyDescriptor;

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
        try {
            instrumentation.logInfo("Initializing Kafka Sink Factory");

            sourceStencilClient = buildSourceStencilClient();
            sinkStencilClient = buildSinkStencilClient();

            sourceMessageDescriptor = sourceStencilClient.get(sinkConfig.getSinkConnectorSchemaProtoMessageClass());
            String sourceKeyClass = sinkConfig.getSinkConnectorSchemaProtoKeyClass();
            sourceKeyDescriptor = (sourceKeyClass != null && !sourceKeyClass.isEmpty())
                    ? sourceStencilClient.get(sourceKeyClass) : null;

            Descriptors.Descriptor sinkMessageDescriptor = sinkStencilClient.get(sinkConfig.getSinkKafkaProtoMessage());
            String sinkKeyClass = sinkConfig.getSinkKafkaProtoKey();
            Descriptors.Descriptor sinkKeyDescriptor = (sinkKeyClass != null && !sinkKeyClass.isEmpty())
                    ? sinkStencilClient.get(sinkKeyClass) : null;

            ProtoMappingParser parser = new ProtoMappingParser(
                    sinkConfig.getSinkKafkaProtoMapping(), sourceMessageDescriptor);

            mappingFunction = new ProtoMappingFunction(
                    parser.getCompiledPrograms(),
                    parser.getSourceBindingName(),
                    sinkMessageDescriptor,
                    sinkKeyDescriptor);

            instrumentation.logInfo("Kafka Sink Factory initialized successfully. Topic: {}, Brokers: {}",
                    sinkConfig.getSinkKafkaTopic(), sinkConfig.getSinkKafkaBrokers());
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception occurred while creating Kafka sink", e);
        }
    }

    public Sink create() {
        KafkaProducer<byte[], byte[]> producer = buildKafkaProducer();
        return new KafkaSink(
                producer,
                mappingFunction,
                sourceMessageDescriptor,
                sourceKeyDescriptor,
                sinkConfig.getSinkKafkaTopic(),
                new Instrumentation(statsDReporter, KafkaSink.class));
    }

    private KafkaProducer<byte[], byte[]> buildKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sinkConfig.getSinkKafkaBrokers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, sinkConfig.getSinkKafkaAcks());
        props.put(ProducerConfig.LINGER_MS_CONFIG, sinkConfig.getSinkKafkaLingerMs());

        if (sinkConfig.isSinkKafkaProduceLargeMessageEnable()) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_REQUEST_SIZE);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        }

        return new KafkaProducer<>(props);
    }

    private StencilClient buildSourceStencilClient() {
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            StencilConfig stencilConfig = StencilConfig.builder()
                    .cacheAutoRefresh(sinkConfig.getSchemaRegistryStencilCacheAutoRefresh())
                    .cacheTtlMs(sinkConfig.getSchemaRegistryStencilCacheTtlMs())
                    .fetchTimeoutMs(sinkConfig.getSchemaRegistryStencilFetchTimeoutMs())
                    .fetchBackoffMinMs(sinkConfig.getSchemaRegistryStencilFetchBackoffMinMs())
                    .fetchRetries(sinkConfig.getSchemaRegistryStencilFetchRetries())
                    .refreshStrategy(sinkConfig.getSchemaRegistryStencilRefreshStrategy())
                    .build();
            return StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        }
        return StencilClientFactory.getClient();
    }

    private StencilClient buildSinkStencilClient() {
        String sinkStencilUrls = sinkConfig.getSinkKafkaSchemaRegistryStencilUrls();
        if (sinkStencilUrls == null || sinkStencilUrls.isEmpty()) {
            return StencilClientFactory.getClient();
        }
        StencilConfig stencilConfig = StencilConfig.builder()
                .cacheAutoRefresh(sinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(sinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                .fetchTimeoutMs((int) sinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                .fetchBackoffMinMs(sinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries((int) sinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries())
                .build();
        return StencilClientFactory.getClient(sinkStencilUrls, stencilConfig);
    }
}
