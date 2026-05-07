package com.gotocompany.depot.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KafkaSinkConfigTest {

    @Test
    public void shouldReadConfigFromEnvVars() {
        Map<String, String> env = new HashMap<>();
        env.put("SINK_KAFKA_BROKERS", "localhost:9092");
        env.put("SINK_KAFKA_TOPIC", "output-topic");
        env.put("SINK_KAFKA_PROTO_MESSAGE", "com.test.OutputMessage");
        env.put("SINK_KAFKA_PROTO_KEY", "com.test.OutputKey");
        env.put("SINK_KAFKA_PROTO_MAPPING", "{\"field1\": \"source.field1\"}");
        env.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://stencil:8080");

        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, env);

        assertEquals("localhost:9092", config.getSinkKafkaBrokers());
        assertEquals("output-topic", config.getSinkKafkaTopic());
        assertEquals("com.test.OutputMessage", config.getSinkKafkaProtoMessage());
        assertEquals("com.test.OutputKey", config.getSinkKafkaProtoKey());
        assertEquals("{\"field1\": \"source.field1\"}", config.getSinkKafkaProtoMapping());
        assertEquals("http://stencil:8080", config.getSinkKafkaSchemaRegistryStencilUrls());
    }

    @Test
    public void shouldHaveCorrectDefaults() {
        Map<String, String> env = new HashMap<>();
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, env);

        assertFalse(config.isSinkKafkaProduceLargeMessageEnable());
        assertFalse(config.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
        assertEquals(86400000L, config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
        assertEquals(10000L, config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
        assertEquals(5000L, config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
        assertEquals(4, config.getSinkKafkaSchemaRegistryStencilFetchRetries());
        assertEquals("all", config.getSinkKafkaAcks());
        assertEquals("0", config.getSinkKafkaLingerMs());
    }
}
