package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaSinkStencilUtilsTest {

    private Map<String, String> properties;

    @Before
    public void setUp() {
        properties = new HashMap<>();
        properties.put("SINK_KAFKA_BROKERS", "broker:9092");
        properties.put("SINK_KAFKA_TOPIC", "output-topic");
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.example.Output");
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{}");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://sink-stencil:8080");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "false");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "600000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "20000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS", "30000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "2");
        properties.put("SCHEMA_REGISTRY_STENCIL_URLS", "http://source-stencil:8080");
        properties.put("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "true");
        properties.put("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "900000");
    }

    @Test
    public void shouldBuildSinkStencilConfigFromKafkaSinkConfig() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilConfig stencilConfig = KafkaSinkStencilUtils.getSinkStencilConfig(
                config, new NoOpStatsDClient());

        Assert.assertFalse(stencilConfig.getCacheAutoRefresh());
        Assert.assertEquals(600000L, (long) stencilConfig.getCacheTtlMs());
        Assert.assertEquals(20000, (int) stencilConfig.getFetchTimeoutMs());
        Assert.assertEquals(30000L, (long) stencilConfig.getFetchBackoffMinMs());
        Assert.assertEquals(2, (int) stencilConfig.getFetchRetries());
    }

    @Test
    public void shouldBuildSourceStencilConfigFromKafkaSinkConfig() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilConfig stencilConfig = KafkaSinkStencilUtils.getSourceStencilConfig(
                config, new NoOpStatsDClient());

        Assert.assertTrue(stencilConfig.getCacheAutoRefresh());
        Assert.assertEquals(900000L, (long) stencilConfig.getCacheTtlMs());
    }

    @Test
    public void shouldUseDifferentConfigsForSourceAndSink() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilConfig sinkConfig = KafkaSinkStencilUtils.getSinkStencilConfig(
                config, new NoOpStatsDClient());
        StencilConfig sourceConfig = KafkaSinkStencilUtils.getSourceStencilConfig(
                config, new NoOpStatsDClient());

        Assert.assertNotEquals(sinkConfig.getCacheAutoRefresh(), sourceConfig.getCacheAutoRefresh());
        Assert.assertNotEquals(sinkConfig.getCacheTtlMs(), sourceConfig.getCacheTtlMs());
    }
}
