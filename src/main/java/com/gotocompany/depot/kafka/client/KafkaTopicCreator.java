package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Ensures the Kafka sink output topic exists, creating it when missing.
 *
 * <p>Topic creation settings ({@code SINK_KAFKA_TOPIC_PARTITION_COUNT},
 * {@code SINK_KAFKA_TOPIC_REPLICATION_FACTOR}, {@code SINK_KAFKA_TOPIC_RETENTION_HR}) are applied only when
 * the topic does not already exist on the broker. When the topic exists those settings are ignored.
 */
public class KafkaTopicCreator {

    private static final long MILLIS_PER_HOUR = 3600_000L;

    private final Function<Properties, AdminClient> adminClientFactory;

    /**
     * Creates a topic creator that builds Kafka {@link AdminClient} instances from properties.
     */
    public KafkaTopicCreator() {
        this(AdminClient::create);
    }

    /**
     * Creates a topic creator with a custom admin client factory.
     *
     * @param adminClientFactory factory used to create admin clients from properties
     */
    public KafkaTopicCreator(Function<Properties, AdminClient> adminClientFactory) {
        this.adminClientFactory = adminClientFactory;
    }

    /**
     * Ensures the configured sink topic exists, creating it when absent.
     *
     * @param sinkConfig          the Kafka sink configuration
     * @param producerProperties  producer properties used to derive admin client connection settings
     * @param instrumentation     instrumentation used for logging
     */
    public void ensureTopicExists(KafkaSinkConfig sinkConfig, Properties producerProperties, Instrumentation instrumentation) {
        Properties adminProperties = createAdminProperties(sinkConfig, producerProperties);
        try (AdminClient adminClient = adminClientFactory.apply(adminProperties)) {
            ensureTopicExists(adminClient, sinkConfig, instrumentation);
        }
    }

    /**
     * Ensures the configured sink topic exists using the provided admin client.
     *
     * @param adminClient     the Kafka admin client
     * @param sinkConfig      the Kafka sink configuration
     * @param instrumentation instrumentation used for logging
     */
    void ensureTopicExists(AdminClient adminClient, KafkaSinkConfig sinkConfig, Instrumentation instrumentation) {
        String topic = sinkConfig.getSinkKafkaTopic();
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topic)) {
                instrumentation.logInfo("Kafka topic {} already exists, skipping topic creation configs", topic);
                return;
            }
            NewTopic newTopic = buildNewTopic(sinkConfig);
            instrumentation.logInfo("Creating kafka topic {} with partitions={}, replicationFactor={}, retentionHr={}",
                    topic,
                    sinkConfig.getSinkKafkaTopicPartitionCount(),
                    sinkConfig.getSinkKafkaTopicReplicationFactor(),
                    sinkConfig.getSinkKafkaTopicRetentionHr());
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
            createTopicsResult.all().get();
            instrumentation.logInfo("Successfully created kafka topic {}", topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(String.format("Interrupted while ensuring kafka topic %s exists", topic), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(String.format("Failed to ensure kafka topic %s exists", topic), e.getCause() == null ? e : e.getCause());
        }
    }

    /**
     * Builds the {@link NewTopic} definition used when auto-creating the sink topic.
     *
     * @param sinkConfig the Kafka sink configuration
     * @return the topic definition to create
     */
    NewTopic buildNewTopic(KafkaSinkConfig sinkConfig) {
        Optional<Integer> partitions = Optional.ofNullable(sinkConfig.getSinkKafkaTopicPartitionCount());
        Optional<Short> replicationFactor = Optional.empty();
        if (sinkConfig.getSinkKafkaTopicReplicationFactor() != null) {
            replicationFactor = Optional.of(sinkConfig.getSinkKafkaTopicReplicationFactor().shortValue());
        }
        NewTopic newTopic = new NewTopic(sinkConfig.getSinkKafkaTopic(), partitions, replicationFactor);
        if (sinkConfig.getSinkKafkaTopicRetentionHr() != null) {
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG,
                    String.valueOf(sinkConfig.getSinkKafkaTopicRetentionHr() * MILLIS_PER_HOUR));
            newTopic.configs(topicConfigs);
        }
        return newTopic;
    }

    /**
     * Builds admin client properties from the sink brokers and producer connection settings.
     *
     * @param sinkConfig         the Kafka sink configuration
     * @param producerProperties producer properties that may include security settings
     * @return admin client properties
     */
    static Properties createAdminProperties(KafkaSinkConfig sinkConfig, Properties producerProperties) {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sinkConfig.getSinkKafkaBrokers());
        if (producerProperties != null) {
            for (String name : producerProperties.stringPropertyNames()) {
                if (isAdminClientProperty(name)) {
                    adminProperties.put(name, producerProperties.getProperty(name));
                }
            }
        }
        return adminProperties;
    }

    private static boolean isAdminClientProperty(String name) {
        return "security.protocol".equals(name)
                || name.startsWith("sasl.")
                || name.startsWith("ssl.")
                || AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG.equals(name)
                || AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG.equals(name)
                || AdminClientConfig.CLIENT_ID_CONFIG.equals(name)
                || ProducerConfig.BOOTSTRAP_SERVERS_CONFIG.equals(name);
    }
}
