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
        assertFalse(config.isSinkKafkaSchemaRegistryStencilEnable());
        assertEquals("", config.getSinkKafkaSchemaRegistryStencilUrls());
        assertTrue(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
        assertNull(config.getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken());
        assertEquals(Long.valueOf(900000L), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
        assertEquals(Integer.valueOf(10000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
        assertEquals(Long.valueOf(60000L), config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
        assertEquals(Integer.valueOf(4), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
        assertNotNull(config.getSinkKafkaSchemaRegistryStencilRefreshStrategy());
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
