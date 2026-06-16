package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.config.KafkaSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KafkaProducerPropertiesFactoryTest {

    private Map<String, String> getBaseConfig() {
        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_KAFKA_BROKERS", "localhost:9092");
        configMap.put("SINK_KAFKA_TOPIC", "output-topic");
        configMap.put("SINK_KAFKA_PROTO_MESSAGE", "com.gotocompany.depot.TestKafkaOutputMessage");
        configMap.put("SINK_KAFKA_PROTO_KEY", "com.gotocompany.depot.TestKafkaOutputKey");
        configMap.put("SINK_KAFKA_PROTO_MAPPING", "{\"user_id\": \"source.account_go_id\"}");
        configMap.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS", "http://localhost:8000");
        configMap.put("SINK_KAFKA_STREAM", "test-stream");
        return configMap;
    }

    @Test
    public void shouldCreateBaseProducerProperties() {
        Map<String, String> configMap = getBaseConfig();
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertEquals("localhost:9092", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(), properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(), properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertFalse(properties.containsKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
        assertFalse(properties.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    }

    @Test
    public void shouldNotPassReservedConfigsToProducer() {
        Map<String, String> configMap = getBaseConfig();
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertFalse(properties.containsKey("topic"));
        assertFalse(properties.containsKey("brokers"));
        assertFalse(properties.containsKey("proto.message"));
        assertFalse(properties.containsKey("proto.key"));
        assertFalse(properties.containsKey("proto.mapping"));
        assertFalse(properties.containsKey("schema.registry.stencil.urls"));
        assertFalse(properties.containsKey("stream"));
        assertFalse(properties.containsKey("produce.large.message.enable"));
    }

    @Test
    public void shouldPassThroughAdditionalSinkKafkaConfigsToProducer() {
        Map<String, String> configMap = getBaseConfig();
        configMap.put("SINK_KAFKA_LINGER_MS", "10");
        configMap.put("SINK_KAFKA_SASL_JAAS_CONFIG", "org.apache.kafka.common.security.plain.PlainLoginModule required;");
        configMap.put("SINK_KAFKA_ACKS", "all");
        configMap.put("SOME_OTHER_CONFIG", "value");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertEquals("10", properties.get("linger.ms"));
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required;", properties.get("sasl.jaas.config"));
        assertEquals("all", properties.get("acks"));
        assertFalse(properties.containsKey("some.other.config"));
    }

    @Test
    public void shouldSetLargeMessagePropertiesWhenEnabled() {
        Map<String, String> configMap = getBaseConfig();
        configMap.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertEquals("20971520", properties.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
        assertEquals("snappy", properties.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    }

    @Test
    public void shouldPreferLargeMessagePropertiesOverPassThroughConfigs() {
        Map<String, String> configMap = getBaseConfig();
        configMap.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        configMap.put("SINK_KAFKA_MAX_REQUEST_SIZE", "100");
        configMap.put("SINK_KAFKA_COMPRESSION_TYPE", "gzip");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertEquals("20971520", properties.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
        assertEquals("snappy", properties.get(ProducerConfig.COMPRESSION_TYPE_CONFIG));
    }

    @Test
    public void shouldPreferConfiguredBrokersOverPassThroughConfigs() {
        Map<String, String> configMap = getBaseConfig();
        configMap.put("SINK_KAFKA_BOOTSTRAP_SERVERS", "otherhost:9092");
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, configMap);
        assertEquals("localhost:9092", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void shouldCreateOnlyBasePropertiesForEmptyConfigMap() {
        Map<String, String> configMap = getBaseConfig();
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        Properties properties = KafkaProducerPropertiesFactory.create(sinkConfig, new HashMap<>());
        assertEquals(3, properties.size());
        assertEquals("localhost:9092", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(), properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(), properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
}
