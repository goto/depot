package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.config.KafkaSinkConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Builds the Kafka producer properties from the sink configuration and the pass-through environment.
 *
 * <p>Any {@code SINK_KAFKA_} variable that is not a reserved sink key or a Stencil-prefixed key is forwarded
 * to the producer as a dotted, lower cased property name. Explicit producer settings from
 * {@link KafkaSinkConfig}, large message mode and the mandatory bootstrap servers are applied last so that
 * they take precedence over pass-through values.
 */
public final class KafkaProducerPropertiesFactory {

    /**
     * Maximum request size in bytes applied when large message mode is enabled.
     */
    public static final int LARGE_MESSAGE_MAX_REQUEST_SIZE = 20971520;
    /**
     * Compression type applied when large message mode is enabled.
     */
    public static final String LARGE_MESSAGE_COMPRESSION_TYPE = "snappy";

    private static final String SINK_KAFKA_PREFIX = "SINK_KAFKA_";
    private static final String SINK_KAFKA_STENCIL_PREFIX = "SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_";
    private static final Set<String> RESERVED_CONFIG_KEYS = new HashSet<>(Arrays.asList(
            "SINK_KAFKA_BROKERS",
            "SINK_KAFKA_TOPIC",
            "SINK_KAFKA_PROTO_MESSAGE",
            "SINK_KAFKA_PROTO_KEY",
            "SINK_KAFKA_PROTO_MAPPING",
            "SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE",
            "SINK_KAFKA_STREAM",
            "SINK_KAFKA_ACKS",
            "SINK_KAFKA_BATCH_SIZE",
            "SINK_KAFKA_BUFFER_MEMORY",
            "SINK_KAFKA_KEY_SERIALIZER",
            "SINK_KAFKA_LINGER_MS",
            "SINK_KAFKA_RETRIES",
            "SINK_KAFKA_VALUE_SERIALIZER",
            "SINK_KAFKA_TOPIC_PARTITION_COUNT",
            "SINK_KAFKA_TOPIC_REPLICATION_FACTOR",
            "SINK_KAFKA_TOPIC_RETENTION_HR"));

    /**
     * Prevents instantiation of this utility class.
     */
    private KafkaProducerPropertiesFactory() {
    }

    /**
     * Builds the producer properties for the sink.
     *
     * @param sinkConfig the Kafka sink configuration
     * @param configMap  the raw environment used to source pass-through producer properties
     * @return the resolved producer properties
     */
    public static Properties create(KafkaSinkConfig sinkConfig, Map<String, String> configMap) {
        Properties properties = new Properties();
        configMap.entrySet().stream()
                .filter(entry -> isProducerProperty(entry.getKey()))
                .forEach(entry -> properties.put(toProducerPropertyName(entry.getKey()), entry.getValue()));
        if (sinkConfig.isSinkKafkaProduceLargeMessageEnable()) {
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(LARGE_MESSAGE_MAX_REQUEST_SIZE));
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, LARGE_MESSAGE_COMPRESSION_TYPE);
        }
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sinkConfig.getSinkKafkaBrokers());
        properties.put(ProducerConfig.ACKS_CONFIG, sinkConfig.getSinkKafkaAcks());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(sinkConfig.getSinkKafkaBatchSize()));
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(sinkConfig.getSinkKafkaBufferMemory()));
        properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(sinkConfig.getSinkKafkaLingerMs()));
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(sinkConfig.getSinkKafkaRetries()));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, sinkConfig.getSinkKafkaKeySerializer());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, sinkConfig.getSinkKafkaValueSerializer());
        return properties;
    }

    /**
     * Returns whether a configuration key should be forwarded to the producer.
     *
     * @param configKey the configuration key to test
     * @return {@code true} if the key is a forwardable producer property, {@code false} otherwise
     */
    private static boolean isProducerProperty(String configKey) {
        if (configKey == null || !configKey.startsWith(SINK_KAFKA_PREFIX)) {
            return false;
        }
        return !RESERVED_CONFIG_KEYS.contains(configKey) && !configKey.startsWith(SINK_KAFKA_STENCIL_PREFIX);
    }

    /**
     * Converts a sink configuration key into its Kafka producer property name.
     *
     * @param configKey the sink configuration key
     * @return the dotted, lower cased producer property name
     */
    private static String toProducerPropertyName(String configKey) {
        return configKey.substring(SINK_KAFKA_PREFIX.length()).toLowerCase(Locale.ROOT).replace('_', '.');
    }
}
