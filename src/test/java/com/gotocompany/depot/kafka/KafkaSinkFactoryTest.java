package com.gotocompany.depot.kafka;

import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KafkaSinkFactoryTest {

    private Map<String, String> properties;
    private StatsDReporter statsDReporter;

    @Before
    public void setUp() {
        properties = new HashMap<>();
        properties.put("SINK_KAFKA_BROKERS", "localhost:9092");
        properties.put("SINK_KAFKA_TOPIC", "output-topic");
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.gotocompany.depot.TestMessage");
        properties.put("SINK_KAFKA_PROTO_KEY", "com.gotocompany.depot.TestKey");
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{\"order_number\": \"source.order_number\"}");
        properties.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", "com.gotocompany.depot.TestMessage");
        statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    @Test
    public void shouldInitializeFactory() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        KafkaSinkFactory factory = new KafkaSinkFactory(config, statsDReporter);
        factory.init();
        Assert.assertNotNull(factory.getSinkStencilClient());
    }

    @Test
    public void shouldInitializeFactoryWithoutStencilUrls() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        KafkaSinkFactory factory = new KafkaSinkFactory(config, statsDReporter);
        factory.init();
        Assert.assertNotNull(factory.getSinkStencilClient());
    }

    @Test
    public void shouldCreateFactoryWithNoOpReporter() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        KafkaSinkFactory factory = new KafkaSinkFactory(config);
        factory.init();
        Assert.assertNotNull(factory.getSinkStencilClient());
    }
}
