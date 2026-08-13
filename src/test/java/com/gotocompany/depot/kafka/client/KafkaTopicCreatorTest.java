package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaTopicCreatorTest {

    private AdminClient adminClient;
    private Instrumentation instrumentation;
    private KafkaTopicCreator topicCreator;
    private Map<String, String> configMap;

    @Before
    public void setUp() {
        adminClient = mock(AdminClient.class);
        instrumentation = mock(Instrumentation.class);
        topicCreator = new KafkaTopicCreator();
        configMap = new HashMap<>();
        configMap.put("SINK_KAFKA_BROKERS", "localhost:9092");
        configMap.put("SINK_KAFKA_TOPIC", "output-topic");
    }

    @Test
    public void shouldSkipCreationWhenTopicAlreadyExists() throws Exception {
        stubExistingTopics(Collections.singleton("output-topic"));
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);

        topicCreator.ensureTopicExists(adminClient, sinkConfig, instrumentation);

        verify(adminClient, never()).createTopics(anyCollection());
    }

    @Test
    public void shouldCreateTopicWithDefaultPartitionCountWhenMissing() throws Exception {
        stubExistingTopics(Collections.<String>emptySet());
        stubCreateTopicsSuccess();
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);

        topicCreator.ensureTopicExists(adminClient, sinkConfig, instrumentation);

        NewTopic newTopic = captureCreatedTopic();
        assertEquals("output-topic", newTopic.name());
        assertEquals(3, newTopic.numPartitions());
        assertEquals(-1, newTopic.replicationFactor());
        assertTrue(newTopic.configs() == null || newTopic.configs().isEmpty());
    }

    @Test
    public void shouldCreateTopicWithConfiguredPartitionReplicationAndRetention() throws Exception {
        stubExistingTopics(Collections.<String>emptySet());
        stubCreateTopicsSuccess();
        configMap.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "5");
        configMap.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "2");
        configMap.put("SINK_KAFKA_TOPIC_RETENTION_HR", "24");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);

        topicCreator.ensureTopicExists(adminClient, sinkConfig, instrumentation);

        NewTopic newTopic = captureCreatedTopic();
        assertEquals(5, newTopic.numPartitions());
        assertEquals(2, newTopic.replicationFactor());
        assertEquals("86400000", newTopic.configs().get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    public void shouldFailWhenTopicCreationFails() throws Exception {
        stubExistingTopics(Collections.<String>emptySet());
        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        @SuppressWarnings("unchecked")
        KafkaFuture<Void> failedFuture = mock(KafkaFuture.class);
        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(failedFuture);
        doThrow(new java.util.concurrent.ExecutionException(new RuntimeException("create failed")))
                .when(failedFuture).get();
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> topicCreator.ensureTopicExists(adminClient, sinkConfig, instrumentation));
        assertTrue(exception.getMessage().contains("output-topic"));
    }

    @Test
    public void shouldBuildNewTopicWithoutRetentionWhenNotConfigured() {
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        NewTopic newTopic = topicCreator.buildNewTopic(sinkConfig);
        assertEquals(3, newTopic.numPartitions());
        assertEquals(-1, newTopic.replicationFactor());
        assertTrue(newTopic.configs() == null || newTopic.configs().isEmpty());
    }

    @Test
    public void shouldCreateAdminPropertiesWithSecuritySettings() {
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties producerProperties = new Properties();
        producerProperties.put("security.protocol", "SASL_PLAINTEXT");
        producerProperties.put("sasl.mechanism", "PLAIN");
        producerProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required;");
        producerProperties.put("acks", "all");
        producerProperties.put("linger.ms", "1000");

        Properties adminProperties = KafkaTopicCreator.createAdminProperties(sinkConfig, producerProperties);

        assertEquals("localhost:9092", adminProperties.get("bootstrap.servers"));
        assertEquals("SASL_PLAINTEXT", adminProperties.get("security.protocol"));
        assertEquals("PLAIN", adminProperties.get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required;", adminProperties.get("sasl.jaas.config"));
        assertFalse(adminProperties.containsKey("acks"));
        assertFalse(adminProperties.containsKey("linger.ms"));
    }

    private void stubExistingTopics(Set<String> topics) throws Exception {
        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(adminClient.listTopics()).thenReturn(listTopicsResult);
        when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(new HashSet<>(topics)));
    }

    private void stubCreateTopicsSuccess() {
        CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
        when(adminClient.createTopics(anyCollection())).thenReturn(createTopicsResult);
        when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    }

    @SuppressWarnings("unchecked")
    private NewTopic captureCreatedTopic() {
        ArgumentCaptor<java.util.Collection<NewTopic>> captor = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(adminClient).createTopics(captor.capture());
        return captor.getValue().iterator().next();
    }
}
