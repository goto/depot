package com.gotocompany.depot.kafka;

import com.gotocompany.depot.Sink;
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
        properties.put("SINK_KAFKA_PROTO_MESSAGE", "com.gojek.esb.booking.BookingLogMessage");
        properties.put("SINK_KAFKA_PROTO_KEY", "com.gojek.esb.booking.BookingLogKey");
        properties.put("SINK_KAFKA_PROTO_MAPPING", "{\"order_id\": \"source.order_number\"}");
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
    public void shouldCreateSinkInstance() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        KafkaSinkFactory factory = new KafkaSinkFactory(config, statsDReporter);
        factory.init();
        Sink sink = factory.create();
        Assert.assertNotNull(sink);
        Assert.assertTrue(sink instanceof KafkaSink);
    }

    @Test
    public void shouldCreateFactoryWithNoOpReporter() {
        KafkaSinkConfig config = ConfigFactory.create(KafkaSinkConfig.class, properties);
        KafkaSinkFactory factory = new KafkaSinkFactory(config);
        factory.init();
        Sink sink = factory.create();
        Assert.assertNotNull(sink);
    }
}
