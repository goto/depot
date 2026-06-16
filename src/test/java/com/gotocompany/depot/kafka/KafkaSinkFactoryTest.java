package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class KafkaSinkFactoryTest {

    private Map<String, String> configMap;
    private StatsDReporter statsDReporter;

    @Before
    public void setup() {
        configMap = new HashMap<>();
        configMap.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestKafkaSourceMessage");
        configMap.put("SINK_KAFKA_BROKERS", "localhost:9092");
        configMap.put("SINK_KAFKA_TOPIC", "output-topic");
        configMap.put("SINK_KAFKA_PROTO_MESSAGE", "com.gotocompany.depot.TestKafkaOutputMessage");
        configMap.put("SINK_KAFKA_PROTO_KEY", "com.gotocompany.depot.TestKafkaOutputKey");
        configMap.put("SINK_KAFKA_PROTO_MAPPING",
                "{\"order_id\": \"string(source.order_number)\", \"user_id\": \"source.account_go_id\", \"order_number\": \"source.order_number\"}");
        statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    @Test
    public void shouldInitializeAndCreateKafkaSink() throws Exception {
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        factory.init();
        Sink sink = factory.create();
        assertNotNull(sink);
        assertTrue(sink instanceof KafkaSink);
        sink.close();
    }

    @Test
    public void shouldCreateFactoryFromTypedConfig() throws Exception {
        KafkaSinkConfig sinkConfig = ConfigFactory.create(KafkaSinkConfig.class, configMap);
        KafkaSinkFactory factory = new KafkaSinkFactory(sinkConfig);
        factory.init();
        Sink sink = factory.create();
        assertNotNull(sink);
        sink.close();
    }

    @Test
    public void shouldFailWhenBrokersAreMissing() {
        configMap.remove("SINK_KAFKA_BROKERS");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause() instanceof ConfigurationException);
        assertEquals("config SINK_KAFKA_BROKERS should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenTopicIsMissing() {
        configMap.remove("SINK_KAFKA_TOPIC");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_TOPIC should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenOutputProtoMessageIsMissing() {
        configMap.remove("SINK_KAFKA_PROTO_MESSAGE");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_PROTO_MESSAGE should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenProtoMappingIsEmpty() {
        configMap.remove("SINK_KAFKA_PROTO_MAPPING");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_PROTO_MAPPING should contain at least one field mapping", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailForNonProtobufSchemaDataType() {
        configMap.put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", "JSON");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("kafka sink only supports PROTOBUF schema data type", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenSourceProtoClassIsNotFound() {
        configMap.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.UnknownMessage");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("com.gotocompany.depot.UnknownMessage"));
    }

    @Test
    public void shouldFailWhenSinkProtoClassIsNotFound() {
        configMap.put("SINK_KAFKA_PROTO_MESSAGE", "com.gotocompany.depot.UnknownOutputMessage");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("com.gotocompany.depot.UnknownOutputMessage"));
    }

    @Test
    public void shouldFailWhenMappedFieldIsUnknown() {
        configMap.put("SINK_KAFKA_PROTO_MAPPING", "{\"unknown_field\": \"source.account_go_id\"}");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("unknown_field"));
    }

    @Test
    public void shouldFailWhenTopicIsWhitespaceOnly() {
        configMap.put("SINK_KAFKA_TOPIC", "   ");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_TOPIC should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenBrokersAreEmptyString() {
        configMap.put("SINK_KAFKA_BROKERS", "");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_BROKERS should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldFailWhenKeyProtoClassIsNotFound() {
        configMap.put("SINK_KAFKA_PROTO_KEY", "com.gotocompany.depot.UnknownOutputKey");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("com.gotocompany.depot.UnknownOutputKey"));
    }

    @Test
    public void shouldFailWhenProtoMappingIsInvalidJson() {
        configMap.put("SINK_KAFKA_PROTO_MAPPING", "{invalid-json");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("SINK_KAFKA_PROTO_MAPPING is not a valid JSON object"));
    }

    @Test
    public void shouldFailWhenMappingExpressionIsInvalid() {
        configMap.put("SINK_KAFKA_PROTO_MAPPING", "{\"order_id\": \"source.not_a_field\"}");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertTrue(exception.getCause().getMessage().contains("not_a_field"));
    }

    @Test
    public void shouldFailWhenStencilEnabledButUrlsMissing() {
        configMap.put("SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_ENABLE", "true");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, factory::init);
        assertEquals("config SINK_KAFKA_SCHEMA_REGISTRY_STENCIL_URLS should not be empty", exception.getCause().getMessage());
    }

    @Test
    public void shouldInitializeAndCreateKafkaSinkWithoutKeyProto() throws Exception {
        configMap.remove("SINK_KAFKA_PROTO_KEY");
        configMap.put("SINK_KAFKA_PROTO_MAPPING", "{\"order_id\": \"string(source.order_number)\"}");
        KafkaSinkFactory factory = new KafkaSinkFactory(configMap, statsDReporter);
        factory.init();
        Sink sink = factory.create();
        assertNotNull(sink);
        sink.close();
    }
}
