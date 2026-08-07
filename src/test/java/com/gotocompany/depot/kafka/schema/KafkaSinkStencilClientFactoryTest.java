package com.gotocompany.depot.kafka.schema;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.stencil.SchemaUpdateListener;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.client.URLStencilClient;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.NoOpStatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaSinkStencilClientFactoryTest {

    @Test
    public void shouldCreateClassLoadStencilClientWhenStencilIsDisabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE", "false");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilClient stencilClient = KafkaSinkStencilClientFactory.create(sinkConfig, new NoOpStatsDClient(), null);
        assertTrue(stencilClient instanceof ClassLoadStencilClient);
    }

    @Test
    public void shouldCreateUrlStencilClientWhenStencilIsEnabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE", "true");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://localhost:8000/descriptors");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilClient stencilClient = KafkaSinkStencilClientFactory.create(sinkConfig, new NoOpStatsDClient(), null);
        assertTrue(stencilClient instanceof URLStencilClient);
    }

    @Test
    public void shouldBuildStencilConfigFromSinkConfigs() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "false");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "120000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "5000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS", "7000");
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "2");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, properties);
        SchemaUpdateListener updateListener = Mockito.mock(SchemaUpdateListener.class);
        StencilConfig stencilConfig = KafkaSinkStencilClientFactory.getStencilConfig(sinkConfig, new NoOpStatsDClient(), updateListener);
        assertEquals(false, stencilConfig.getCacheAutoRefresh());
        assertEquals(Long.valueOf(120000L), stencilConfig.getCacheTtlMs());
        assertEquals(Integer.valueOf(5000), stencilConfig.getFetchTimeoutMs());
        assertEquals(Long.valueOf(7000L), stencilConfig.getFetchBackoffMinMs());
        assertEquals(Integer.valueOf(2), stencilConfig.getFetchRetries());
        assertEquals(updateListener, stencilConfig.getUpdateListener());
        assertTrue(stencilConfig.getFetchHeaders().isEmpty());
    }

    @Test
    public void shouldAddAuthorizationHeaderWhenBearerTokenIsConfigured() {
        Map<String, String> properties = new HashMap<>();
        properties.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_FETCH_AUTH_BEARER_TOKEN", "token-123");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, properties);
        StencilConfig stencilConfig = KafkaSinkStencilClientFactory.getStencilConfig(sinkConfig, new NoOpStatsDClient(), null);
        assertEquals(1, stencilConfig.getFetchHeaders().size());
        assertEquals("Authorization", stencilConfig.getFetchHeaders().get(0).getName());
        assertEquals("Bearer token-123", stencilConfig.getFetchHeaders().get(0).getValue());
    }
}
