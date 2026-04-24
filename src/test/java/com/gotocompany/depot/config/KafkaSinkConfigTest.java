package com.gotocompany.depot.config;

import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.ConfigFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaSinkConfigTest {

    private final Map<String, String> properties = new HashMap<>();

    @Before
    public void setUp() {
        properties.clear();
    }

    @After
    public void tearDown() {
        properties.clear();
    }

    private KafkaSinkConfig createConfig() {
        return ConfigFactory.create(KafkaSinkConfig.class, properties);
    }

    @Test
    public void shouldReadSinkKafkaBrokers() {
        properties.put("SINK_KAFKA_BROKERS", "broker1:9092,broker2:9092");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("broker1:9092,broker2:9092", config.getSinkKafkaBrokers());
    }

    @Test
    public void shouldReadSinkKafkaTopic() {
        properties.put("SINK_KAFKA_TOPIC", "output-topic");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("output-topic", config.getSinkKafkaTopic());
    }

    @Test
    public void shouldReadSinkKafkaProtoMessage() {
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.gojek.esb.booking.BookingLogMessage");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("com.gojek.esb.booking.BookingLogMessage", config.getSinkKafkaProtoMessage());
    }

    @Test
    public void shouldReadSinkKafkaProtoKey() {
        properties.put("SINK_KAFKA_PROTO_KEY", "com.gojek.esb.booking.BookingLogKey");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("com.gojek.esb.booking.BookingLogKey", config.getSinkKafkaProtoKey());
    }

    @Test
    public void shouldDefaultSinkKafkaProtoKeyToEmpty() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("", config.getSinkKafkaProtoKey());
    }

    @Test
    public void shouldReadSinkKafkaProtoMapping() {
        String mapping = "{\"order_id\": \"source.order_number\", \"user_id\": \"source.account_go_id\"}";
        properties.put("SINK_KAFKA_PROTO_MAPPING", mapping);
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(mapping, config.getSinkKafkaProtoMapping());
    }

    @Test
    public void shouldDefaultProduceLargeMessageToFalse() {
        KafkaSinkConfig config = createConfig();
        Assert.assertFalse(config.isSinkKafkaProduceLargeMessageEnabled());
    }

    @Test
    public void shouldReadProduceLargeMessageEnable() {
        properties.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        KafkaSinkConfig config = createConfig();
        Assert.assertTrue(config.isSinkKafkaProduceLargeMessageEnabled());
    }

    @Test
    public void shouldDefaultSinkKafkaStreamToEmpty() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("", config.getSinkKafkaStream());
    }

    @Test
    public void shouldReadSinkKafkaStream() {
        properties.put("SINK_KAFKA_STREAM", "gjk-p-acc-dagstream");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("gjk-p-acc-dagstream", config.getSinkKafkaStream());
    }

    @Test
    public void shouldReadSinkStencilUrls() {
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://stencil.example.com/v1/schemas/test");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("http://stencil.example.com/v1/schemas/test", config.getSinkKafkaSchemaRegistryStencilUrls());
    }

    @Test
    public void shouldDefaultSinkStencilCacheAutoRefreshToTrue() {
        KafkaSinkConfig config = createConfig();
        Assert.assertTrue(config.getSinkKafkaSchemaRegistryStencilCacheAutoRefresh());
    }

    @Test
    public void shouldDefaultSinkStencilRefreshStrategyToVersionBased() {
        KafkaSinkConfig config = createConfig();
        SchemaRefreshStrategy strategy = config.getSinkKafkaSchemaRegistryStencilRefreshStrategy();
        Assert.assertNotNull(strategy);
    }

    @Test
    public void shouldDefaultSinkStencilFetchAuthBearerTokenToEmpty() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals("", config.getSinkKafkaSchemaRegistryStencilFetchAuthBearerToken());
    }

    @Test
    public void shouldDefaultSinkStencilCacheTtlMs() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Long.valueOf(900000L), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
    }

    @Test
    public void shouldDefaultSinkStencilFetchTimeoutMs() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Integer.valueOf(10000), config.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs());
    }

    @Test
    public void shouldDefaultSinkStencilFetchBackoffMinMs() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Long.valueOf(60000L), config.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs());
    }

    @Test
    public void shouldDefaultSinkStencilFetchRetries() {
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Integer.valueOf(4), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
    }

    @Test
    public void shouldReadSinkStencilFetchRetries() {
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "8");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Integer.valueOf(8), config.getSinkKafkaSchemaRegistryStencilFetchRetries());
    }

    @Test
    public void shouldReadSinkStencilCacheTtlMs() {
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "1800000");
        KafkaSinkConfig config = createConfig();
        Assert.assertEquals(Long.valueOf(1800000L), config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
    }
}
