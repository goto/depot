package com.gotocompany.depot.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaSinkConfigTest {

    @Test
    public void shouldParseKafkaSinkConfigs() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_BROKERS", "localhost:9092,localhost:9093");
        properties.put("SINK_KAFKA_TOPIC", "output-topic");
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.gotocompany.depot.TestKafkaOutputMessage");
        properties.put("SINK_KAFKA_PROTO_KEY", "com.gotocompany.depot.TestKafkaOutputKey");
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{\"user_id\": \"source.account_go_id\"}");
        properties.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertEquals("localhost:9092,localhost:9093", config.getSinkKafkaBrokers());
        assertEquals("output-topic", config.getSinkKafkaTopic());
        assertEquals("com.gotocompany.depot.TestKafkaOutputMessage", config.getSinkKafkaProtoMessage());
        assertEquals("com.gotocompany.depot.TestKafkaOutputKey", config.getSinkKafkaProtoKey());
        assertEquals(1, config.getSinkKafkaProtoMapping().size());
        assertEquals("source.account_go_id", config.getSinkKafkaProtoMapping().get("user_id"));
        assertTrue(config.isSinkKafkaProduceLargeMessageEnable());
    }

    @Test
    public void shouldReturnDefaultsWhenConfigsAreNotSet() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, new HashMap<String, String>());
        assertNull(config.getSinkKafkaProtoKey());
        assertTrue(config.getSinkKafkaProtoMapping().isEmpty());
        assertFalse(config.isSinkKafkaProduceLargeMessageEnable());
        assertTrue(config.isSinkKafkaSchemaRegistryStencilEnable());
        assertEquals("", config.getSinkKafkaSchemaRegistryStencilUrls());
        assertTrue(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
        assertNull(config.getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken());
        assertEquals(Long.valueOf(900000L), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
        assertEquals(Integer.valueOf(10000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
        assertEquals(Long.valueOf(60000L), config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
        assertEquals(Integer.valueOf(4), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
        assertNotNull(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy());
        assertEquals("all", config.getSinkKafkaAcks());
        assertEquals(Integer.valueOf(16384), config.getSinkKafkaBatchSize());
        assertEquals(Long.valueOf(33554432L), config.getSinkKafkaBufferMemory());
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", config.getSinkKafkaKeySerializer());
        assertEquals(Integer.valueOf(1000), config.getSinkKafkaLingerMs());
        assertEquals(Integer.valueOf(2147483647), config.getSinkKafkaRetries());
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", config.getSinkKafkaValueSerializer());
        assertEquals(Integer.valueOf(3), config.getSinkKafkaTopicPartitionCount());
        assertNull(config.getSinkKafkaTopicReplicationFactor());
        assertNull(config.getSinkKafkaTopicRetentionHr());
    }

    @Test
    public void shouldParseProducerAndTopicCreationConfigs() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_ACKS", "1");
        properties.put("SINK_KAFKA_BATCH_SIZE", "32768");
        properties.put("SINK_KAFKA_BUFFER_MEMORY", "67108864");
        properties.put("SINK_KAFKA_KEY_SERIALIZER", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("SINK_KAFKA_LINGER_MS", "5");
        properties.put("SINK_KAFKA_RETRIES", "10");
        properties.put("SINK_KAFKA_VALUE_SERIALIZER", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "6");
        properties.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "2");
        properties.put("SINK_KAFKA_TOPIC_RETENTION_HR", "48");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertEquals("1", config.getSinkKafkaAcks());
        assertEquals(Integer.valueOf(32768), config.getSinkKafkaBatchSize());
        assertEquals(Long.valueOf(67108864L), config.getSinkKafkaBufferMemory());
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", config.getSinkKafkaKeySerializer());
        assertEquals(Integer.valueOf(5), config.getSinkKafkaLingerMs());
        assertEquals(Integer.valueOf(10), config.getSinkKafkaRetries());
        assertEquals("org.apache.kafka.common.serialization.StringSerializer", config.getSinkKafkaValueSerializer());
        assertEquals(Integer.valueOf(6), config.getSinkKafkaTopicPartitionCount());
        assertEquals(Integer.valueOf(2), config.getSinkKafkaTopicReplicationFactor());
        assertEquals(Integer.valueOf(48), config.getSinkKafkaTopicRetentionHr());
    }

    @Test
    public void shouldParseSinkStencilConfigs() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE", "true");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://localhost:8000/descriptors");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "false");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY", "LONG_POLLING");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN", "token-123");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "120000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "5000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS", "7000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "2");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertTrue(config.isSinkKafkaSchemaRegistryStencilEnable());
        assertEquals("http://localhost:8000/descriptors", config.getSinkKafkaSchemaRegistryStencilUrls());
        assertFalse(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
        assertEquals("token-123", config.getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken());
        assertEquals(Long.valueOf(120000L), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
        assertEquals(Integer.valueOf(5000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
        assertEquals(Long.valueOf(7000L), config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
        assertEquals(Integer.valueOf(2), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
        assertNotNull(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy());
    }

    @Test
    public void shouldTrimConfigValues() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_TOPIC", "  output-topic  ");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertEquals("output-topic", config.getSinkKafkaTopic());
    }

    @Test
    public void shouldReturnNullProtoKeyForWhitespaceOnlyValue() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_PROTO_KEY", "   ");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertNull(config.getSinkKafkaProtoKey());
    }

    @Test
    public void shouldFailOnAccessOfInvalidProtoMapping() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{invalid-json");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        assertThrows(com.gotocompany.depot.exception.ConfigurationException.class, config::getSinkKafkaProtoMapping);
    }
}
