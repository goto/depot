package com.gotocompany.depot.kafka;

import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaSinkConfigTest {

    @Test
    public void shouldLoadRequiredKafkaConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put("SINK_KAFKA_BROKERS", "broker1:9092,broker2:9092");
        props.put("SINK_KAFKA_TOPIC", "output-topic");
        props.put("SINK_KAFKA_PROTO_MESSAGE", "com.example.OutputMessage");
        props.put("SINK_KAFKA_PROTO_KEY", "com.example.OutputKey");
        props.put("SINK_KAFKA_PROTO_MAPPING", "{\"field_a\": \"source.field_x\"}");

        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, props);

        assertEquals("broker1:9092,broker2:9092", config.getSinkKafkaBrokers());
        assertEquals("output-topic", config.getSinkKafkaTopic());
        assertEquals("com.example.OutputMessage", config.getSinkKafkaProtoMessage());
        assertEquals("com.example.OutputKey", config.getSinkKafkaProtoKey());
        assertEquals("{\"field_a\": \"source.field_x\"}", config.getSinkKafkaProtoMapping());
    }

    @Test
    public void shouldDefaultToFalseForLargeMessageEnable() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, new HashMap<>());
        assertFalse(config.isSinkKafkaProduceLargeMessageEnable());
    }

    @Test
    public void shouldEnableLargeMessageWhenConfigured() {
        Map<String, String> props = new HashMap<>();
        props.put("SINK_KAFKA_PRODUCE_LARGE_MESSAGE_ENABLE", "true");
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, props);
        assertTrue(config.isSinkKafkaProduceLargeMessageEnable());
    }

    @Test
    public void shouldDefaultStencilCacheTtl() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, new HashMap<>());
        assertEquals(900000L, config.getSinkKafkaSchemaRegistryStencilCacheTtlMs());
    }
}
