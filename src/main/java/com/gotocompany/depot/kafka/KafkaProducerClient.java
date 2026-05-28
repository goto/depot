package com.gotocompany.depot.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProducerClient implements Closeable {

    private static final int LARGE_MESSAGE_MAX_REQUEST_BYTES = 20971520;

    private final KafkaProducer<byte[], byte[]> producer;
    private final String topic;

    public KafkaProducerClient(KafkaSinkConfig config, Map<String, String> allEnvVars) {
        this.topic = config.getSinkKafkaTopic();
        this.producer = new KafkaProducer<>(buildProperties(config, allEnvVars));
    }

    /**
     * Sends key+value bytes synchronously and returns normally on success.
     *
     * @throws RuntimeException on producer send/ack failure; callers map this to ErrorInfo.
     */
    public void send(byte[] key, byte[] value) {
        try {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Kafka send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Kafka send failed: " + e.getCause().getMessage(), e.getCause());
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }

    private static Properties buildProperties(KafkaSinkConfig config, Map<String, String> allEnvVars) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getSinkKafkaBrokers());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty("allow.auto.create.topics", "true");

        if (config.isSinkKafkaProduceLargeMessageEnable()) {
            props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(LARGE_MESSAGE_MAX_REQUEST_BYTES));
            props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        }

        // Pass through any SINK_KAFKA_* env vars as Kafka producer properties.
        // Keys are lowercased by stripping the SINK_KAFKA_ prefix and replacing _ with .
        // e.g. SINK_KAFKA_LINGER_MS -> linger.ms
        final String prefix = "SINK_KAFKA_";
        final java.util.Set<String> knownConfigKeys = java.util.Arrays.asList(
                "BROKERS", "TOPIC", "PROTO_MESSAGE", "PROTO_KEY", "PROTO_MAPPING",
                "PRODUCE_LARGE_MESSAGE_ENABLE", "STREAM",
                "SCHEMA_REGISTRY_STENCIL_URLS", "SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH",
                "SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS",
                "SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS",
                "SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY", "SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN",
                "SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS"
        ).stream().collect(java.util.stream.Collectors.toSet());

        for (Map.Entry<String, String> entry : allEnvVars.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                String suffix = key.substring(prefix.length());
                if (!knownConfigKeys.contains(suffix)) {
                    String producerKey = suffix.toLowerCase().replace('_', '.');
                    props.setProperty(producerKey, entry.getValue());
                    log.debug("Passthrough Kafka producer property: {} = {}", producerKey, entry.getValue());
                }
            }
        }
        return props;
    }
}
