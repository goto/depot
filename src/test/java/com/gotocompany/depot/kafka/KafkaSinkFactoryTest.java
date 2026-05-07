package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkFactoryTest {

    @Mock
    private KafkaSinkConfig kafkaSinkConfig;
    @Mock
    private StatsDReporter statsDReporter;

    @Test
    public void shouldCreateFactoryWithConfig() {
        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        assertNotNull(factory);
    }

    @Test
    public void shouldCreateFactoryWithoutStatsDReporter() {
        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig);
        assertNotNull(factory);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInitCalledWithNullBrokers() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn(null);
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.test.Output");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn("{}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInitCalledWithInvalidProtoClass() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.nonexistent.ProtoClass");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn("{}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.also.nonexistent.ProtoClass");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenInitCalledWithInvalidMappingJson() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn("not-valid-json");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();
    }

    @Test
    public void shouldInitAndCreateSinkSuccessfully() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoKey()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn(
                "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);
        when(kafkaSinkConfig.getSinkKafkaAcks()).thenReturn("all");
        when(kafkaSinkConfig.getSinkKafkaLingerMs()).thenReturn("0");
        when(kafkaSinkConfig.isSinkKafkaProduceLargeMessageEnable()).thenReturn(false);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();

        assertNotNull(factory.create());
    }

    @Test
    public void shouldInitWithLargeMessageEnabled() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoKey()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn(
                "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);
        when(kafkaSinkConfig.getSinkKafkaAcks()).thenReturn("all");
        when(kafkaSinkConfig.getSinkKafkaLingerMs()).thenReturn("5");
        when(kafkaSinkConfig.isSinkKafkaProduceLargeMessageEnable()).thenReturn(true);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();

        assertNotNull(factory.create());
    }

    @Test
    public void shouldInitWithKeyProtoClass() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoKey()).thenReturn("com.gotocompany.depot.TestKey");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn(
                "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("com.gotocompany.depot.TestKey");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);
        when(kafkaSinkConfig.getSinkKafkaAcks()).thenReturn("all");
        when(kafkaSinkConfig.getSinkKafkaLingerMs()).thenReturn("0");
        when(kafkaSinkConfig.isSinkKafkaProduceLargeMessageEnable()).thenReturn(false);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();

        assertNotNull(factory.create());
    }

    @Test
    public void shouldInitWithSchemaRegistryEnabled() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoKey()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn(
                "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(true);
        when(kafkaSinkConfig.getSchemaRegistryStencilUrls()).thenReturn("http://sr:8081");
        when(kafkaSinkConfig.getSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSchemaRegistryStencilCacheTtlMs()).thenReturn(300000L);
        when(kafkaSinkConfig.getSchemaRegistryStencilFetchTimeoutMs()).thenReturn(15000L);
        when(kafkaSinkConfig.getSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(3000L);
        when(kafkaSinkConfig.getSchemaRegistryStencilFetchRetries()).thenReturn(5);
        when(kafkaSinkConfig.getSchemaRegistryStencilRefreshStrategy()).thenReturn("VERSION_BASED_REFRESH");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);
        when(kafkaSinkConfig.getSinkKafkaAcks()).thenReturn("all");
        when(kafkaSinkConfig.getSinkKafkaLingerMs()).thenReturn("0");
        when(kafkaSinkConfig.isSinkKafkaProduceLargeMessageEnable()).thenReturn(false);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();

        assertNotNull(factory.create());
    }

    @Test
    public void shouldCreateMultipleSinksFromSameFactory() {
        when(kafkaSinkConfig.getSinkKafkaBrokers()).thenReturn("localhost:9092");
        when(kafkaSinkConfig.getSinkKafkaTopic()).thenReturn("test-topic");
        when(kafkaSinkConfig.getSinkKafkaProtoMessage()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkKafkaProtoKey()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaProtoMapping()).thenReturn(
                "{\"order_number\": \"com.gotocompany.depot.TestMessage.order_number\"}");
        when(kafkaSinkConfig.isSchemaRegistryStencilEnable()).thenReturn(false);
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoMessageClass()).thenReturn("com.gotocompany.depot.TestMessage");
        when(kafkaSinkConfig.getSinkConnectorSchemaProtoKeyClass()).thenReturn("");
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilUrls()).thenReturn("http://stencil:8080");
        when(kafkaSinkConfig.isSinkKafkaSchemaRegistryStencilCacheAutoRefresh()).thenReturn(false);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilCacheTtlMs()).thenReturn(86400000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchTimeoutMs()).thenReturn(10000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchBackoffMinMs()).thenReturn(5000L);
        when(kafkaSinkConfig.getSinkKafkaSchemaRegistryStencilFetchRetries()).thenReturn(4);
        when(kafkaSinkConfig.getSinkKafkaAcks()).thenReturn("all");
        when(kafkaSinkConfig.getSinkKafkaLingerMs()).thenReturn("0");
        when(kafkaSinkConfig.isSinkKafkaProduceLargeMessageEnable()).thenReturn(false);

        KafkaSinkFactory factory = new KafkaSinkFactory(kafkaSinkConfig, statsDReporter);
        factory.init();

        assertNotNull(factory.create());
        assertNotNull(factory.create());
        assertNotNull(factory.create());
    }
}
