package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import dev.cel.common.CelValidationException;
import dev.cel.runtime.CelEvaluationException;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaSinkFactory {

    private static final Pattern SINK_KAFKA_EXTRA = Pattern.compile("^SINK_KAFKA_(.+)$");
    private static final Set<String> RESERVED = new HashSet<>();

    static {
        RESERVED.add("SINK_KAFKA_BROKERS");
        RESERVED.add("SINK_KAFKA_TOPIC");
        RESERVED.add("SINK_KAFKA_PROTO_MESSAGE");
        RESERVED.add("SINK_KAFKA_PROTO_KEY");
        RESERVED.add("SINK_KAFKA_PROTO_MAPPING");
        RESERVED.add("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE");
        RESERVED.add("SINK_KAFKA_STREAM");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES");
        RESERVED.add("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS");
    }

    private final KafkaSinkConfig config;
    private final StatsDReporter statsDReporter;
    private final StencilClient sourceStencilClient;
    private final Map<String, String> env;

    private ProtoMappingEngine mappingEngine;
    private KafkaProducer<byte[], byte[]> producer;

    public KafkaSinkFactory(StatsDReporter statsDReporter, StencilClient sourceStencilClient, Map<String, String> env) {
        this.statsDReporter = statsDReporter;
        this.sourceStencilClient = sourceStencilClient;
        this.env = env;
        this.config = ConfigFactory.create(KafkaSinkConfig.class, env);
    }

    public void init() throws CelValidationException, CelEvaluationException {
        StencilClient sinkStencil = createSinkStencilClient();
        Descriptors.Descriptor sourceDesc = sourceStencilClient.get(config.getInputSchemaProtoClass());
        Descriptors.Descriptor keyDesc = sinkStencil.get(config.getSinkKafkaProtoKey());
        Descriptors.Descriptor msgDesc = sinkStencil.get(config.getSinkKafkaProtoMessage());
        mappingEngine = ProtoMappingEngine.create(
                config.getSinkKafkaProtoMapping(), sourceDesc, keyDesc, msgDesc);
        producer = new KafkaProducer<>(buildProducerProperties());
    }

    public Sink create() {
        return new KafkaSink(
                producer,
                config.getSinkKafkaTopic(),
                sourceStencilClient,
                config.getInputSchemaProtoClass(),
                mappingEngine,
                new Instrumentation(statsDReporter, KafkaSink.class));
    }

    private StencilClient createSinkStencilClient() {
        StencilConfig stencilConfig = StencilConfig.builder()
                .cacheAutoRefresh(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(config.getSinkKafkaSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDReporter.getClient())
                .fetchHeaders(config.getSinkKafkaSchemaRegistryStencilFetchHeaders())
                .fetchBackoffMinMs(config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(config.getSinkKafkaSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy())
                .build();
        return StencilClientFactory.getClient(config.getSinkKafkaSchemaRegistryStencilUrls(), stencilConfig);
    }

    private Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getSinkKafkaBrokers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");
        if (Boolean.TRUE.equals(config.isSinkKafkaProduceLargeMessageEnable())) {
            props.put("max.request.size", "20971520");
            props.put("compression.type", "snappy");
        }
        if (env != null) {
            env.forEach((key, value) -> {
                if (value == null || RESERVED.contains(key)) {
                    return;
                }
                Matcher m = SINK_KAFKA_EXTRA.matcher(key);
                if (m.matches()) {
                    props.put(m.group(1).replace('_', '.').toLowerCase(), value);
                }
            });
        }
        return props;
    }
}
