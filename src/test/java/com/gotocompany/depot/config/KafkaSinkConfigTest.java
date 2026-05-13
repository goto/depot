package com.gotocompany.depot.config;

import org.aeonbits.owner.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaSinkConfigTest {

    private Map<String, String> properties;

    @Before
    public void setUp() {
        properties = new HashMap<>();
        properties.put("SINK_KAFKA_BROKERS", "broker1:9092,broker2:9092");
        properties.put("SINK_KAFKA_TOPIC", "output-topic");
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.example.OutputMessage");
        properties.put("SINK_KAFKA_PROTO_KEY", "com.example.OutputKey");
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{\"order_id\": \"source.order_number\"}");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://stencil:8080/v1/schemas");
    }

    @After
    public void tearDown() {
        properties.clear();
    }

    @Test
    public void shouldReturnKafkaBrokers() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("broker1:9092,broker2:9092", config.getSinkKafkaBrokers());
    }

    @Test
    public void shouldReturnKafkaTopic() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("output-topic", config.getSinkKafkaTopic());
    }

    @Test
    public void shouldReturnProtoMessage() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("com.example.OutputMessage", config.getSinkKafkaProtoMessage());
    }

    @Test
    public void shouldReturnProtoKey() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("com.example.OutputKey", config.getSinkKafkaProtoKey());
    }

    @Test
    public void shouldReturnEmptyProtoKeyByDefault() {
        properties.remove("SINK_KAFKA_PROTO_KEY");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("", config.getSinkKafkaProtoKey());
    }

    @Test
    public void shouldReturnProtoMapping() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("{\"order_id\": \"source.order_number\"}", config.getSinkKafkaProtoMapping());
    }

    @Test
    public void shouldReturnLargeMessageEnabledDefault() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertFalse(config.isSinkKafkaProduceLargeMessageEnabled());
    }

    @Test
    public void shouldReturnLargeMessageEnabledWhenSet() {
        properties.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertTrue(config.isSinkKafkaProduceLargeMessageEnabled());
    }

    @Test
    public void shouldReturnSinkStencilUrls() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("http://stencil:8080/v1/schemas", config.getSinkKafkaSchemaRegistryStencilUrls());
    }

    @Test
    public void shouldReturnDefaultStencilCacheAutoRefresh() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertTrue(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
    }

    @Test
    public void shouldReturnDefaultStencilCacheTtlMs() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals(Long.valueOf(900000), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
    }

    @Test
    public void shouldReturnDefaultStencilFetchTimeoutMs() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(10000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
    }

    @Test
    public void shouldReturnDefaultStencilFetchRetries() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(4), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
    }

    @Test
    public void shouldReturnDefaultStencilFetchBackoffMinMs() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals(Long.valueOf(60000), config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
    }

    @Test
    public void shouldReturnSinkKafkaStream() {
        properties.put("SINK_KAFKA_STREAM", "gjk-p-acc-dagstream");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("gjk-p-acc-dagstream", config.getSinkKafkaStream());
    }

    @Test
    public void shouldReturnEmptySinkKafkaStreamByDefault() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("", config.getSinkKafkaStream());
    }

    @Test
    public void shouldReturnInputSchemaProtoClass() {
        properties.put("INPUT_SCHEMA_PROTO_CLASS", "com.example.InputMessage");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals("com.example.InputMessage", config.getInputSchemaProtoClass());
    }

    @Test
    public void shouldReturnSourceStencilEnable() {
        properties.put("SCHEMA_REGISTRY_STENCIL_ENABLE", "true");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertTrue(config.isSchemaRegistryStencilEnable());
    }

    @Test
    public void shouldReturnCustomStencilCacheAutoRefresh() {
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "false");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertFalse(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
    }

    @Test
    public void shouldReturnCustomStencilFetchTimeoutMs() {
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "30000");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        Assert.assertEquals(Integer.valueOf(30000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
    }
}
