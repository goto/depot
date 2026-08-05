package com.gotocompany.depot.kafka;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.TestKafkaOutputKey;
import com.gotocompany.depot.TestKafkaOutputMessage;
import com.gotocompany.depot.TestKafkaServiceType;
import com.gotocompany.depot.TestKafkaSinkLocation;
import com.gotocompany.depot.TestKafkaSourceAddress;
import com.gotocompany.depot.TestKafkaSourceLocation;
import com.gotocompany.depot.TestKafkaSourceMessage;
import com.gotocompany.depot.TestKafkaSourceStatus;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end integration tests for the Kafka sink against a real broker.
 *
 * <p>Uses Testcontainers so these tests run on GitHub Actions runners with Docker
 * available. Local Docker is not required for development if CI is used as the
 * verification path.
 */
public class KafkaSinkIntegrationTest {

    private static final String SOURCE_PROTO = "com.gotocompany.depot.TestKafkaSourceMessage";
    private static final String OUTPUT_MESSAGE_PROTO = "com.gotocompany.depot.TestKafkaOutputMessage";
    private static final String OUTPUT_KEY_PROTO = "com.gotocompany.depot.TestKafkaOutputKey";
    private static final String DEFAULT_MAPPING =
            "{\"order_id\": \"string(source.order_number)\", \"user_id\": \"source.account_go_id\", "
                    + "\"order_number\": \"source.order_number\", \"approval_status\": \"source.status.last_status\", "
                    + "\"order_lat\": \"source.order_location.latitude\", "
                    + "\"order_lng\": \"source.order_location.longitude\", "
                    + "\"status\": \"source.current_status == \\\"active\\\" ? \\\"running\\\" : \\\"stopped\\\"\", "
                    + "\"city\": \"has(source.address) ? source.address.city : \\\"unknown\\\"\", "
                    + "\"sink_origin\": \"com.gotocompany.depot.TestKafkaSinkLocation{"
                    + "lat: source.pickup_location.latitude, lng: source.pickup_location.longitude}\", "
                    + "\"is_valid\": \"source.amount > 0.0\", \"order_amount\": \"int(source.amount)\", "
                    + "\"service_type\": \"source.service_type\", \"payload\": \"source.payload\"}";

    @ClassRule
    public static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

    private StatsDReporter statsDReporter;
    private TestKafkaSourceMessage sourceMessage;
    private Sink sink;

    @Before
    public void setUp() {
        statsDReporter = new StatsDReporter(new NoOpStatsDClient());
        sourceMessage = TestKafkaSourceMessage.newBuilder()
                .setOrderNumber(93L)
                .setAccountGoId("user-1")
                .setStatus(TestKafkaSourceStatus.newBuilder().setLastStatus("approved").build())
                .addOrderList(11L).addOrderList(22L).addOrderList(33L)
                .setOrderLocation(TestKafkaSourceLocation.newBuilder().setLatitude(1.5).setLongitude(2.5).build())
                .setCurrentStatus("active")
                .setAddress(TestKafkaSourceAddress.newBuilder().setCity("jakarta").build())
                .setPickupLocation(TestKafkaSourceLocation.newBuilder().setLatitude(3.5).setLongitude(4.5).build())
                .setAmount(42.7)
                .setOrderType(5)
                .setEventTimestamp(Timestamp.newBuilder().setSeconds(1700000000L).setNanos(123).build())
                .setServiceType(TestKafkaServiceType.Enum.GO_FOOD)
                .setPayload(ByteString.copyFromUtf8("abc"))
                .putLabels("k1", "v1")
                .setRetryCount(7)
                .setTotalCount(99L)
                .setFloatAmount(1.25f)
                .addTags("a").addTags("b")
                .build();
    }

    @After
    public void tearDown() throws Exception {
        if (sink != null) {
            sink.close();
            sink = null;
        }
    }

    @Test
    public void shouldProduceMappedMessageWithKeyAndValueToKafka() throws Exception {
        String topic = uniqueTopic("mapped");
        sink = createSink(topic, DEFAULT_MAPPING, true);

        SinkResponse response = sink.pushToSink(Collections.singletonList(
                new Message(null, sourceMessage.toByteArray())));

        assertFalse(response.hasErrors());
        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(topic, 1);
        assertEquals(1, records.size());

        TestKafkaOutputMessage producedValue = TestKafkaOutputMessage.parseFrom(records.get(0).value());
        assertEquals("93", producedValue.getOrderId());
        assertEquals("user-1", producedValue.getUserId());
        assertEquals("approved", producedValue.getApprovalStatus());
        assertEquals(1.5, producedValue.getOrderLat(), 0.0001);
        assertEquals(2.5, producedValue.getOrderLng(), 0.0001);
        assertEquals("running", producedValue.getStatus());
        assertEquals("jakarta", producedValue.getCity());
        assertTrue(producedValue.getIsValid());
        assertEquals(42, producedValue.getOrderAmount());
        assertEquals(TestKafkaServiceType.Enum.GO_FOOD, producedValue.getServiceType());
        assertEquals("abc", producedValue.getPayload().toStringUtf8());
        TestKafkaSinkLocation sinkOrigin = producedValue.getSinkOrigin();
        assertEquals(3.5, sinkOrigin.getLat(), 0.0001);
        assertEquals(4.5, sinkOrigin.getLng(), 0.0001);

        TestKafkaOutputKey producedKey = TestKafkaOutputKey.parseFrom(records.get(0).key());
        assertEquals("93", producedKey.getOrderId());
        assertEquals(93L, producedKey.getOrderNumber());
    }

    @Test
    public void shouldProduceMultipleMessagesInABatch() throws Exception {
        String topic = uniqueTopic("batch");
        sink = createSink(topic, DEFAULT_MAPPING, true);

        TestKafkaSourceMessage second = sourceMessage.toBuilder().setOrderNumber(94L).setAccountGoId("user-2").build();
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, sourceMessage.toByteArray()));
        messages.add(new Message(null, second.toByteArray()));
        messages.add(new Message(null, sourceMessage.toBuilder().setOrderNumber(95L).build().toByteArray()));

        SinkResponse response = sink.pushToSink(messages);

        assertFalse(response.hasErrors());
        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(topic, 3);
        assertEquals(3, records.size());
        assertEquals("93", TestKafkaOutputMessage.parseFrom(records.get(0).value()).getOrderId());
        assertEquals("94", TestKafkaOutputMessage.parseFrom(records.get(1).value()).getOrderId());
        assertEquals("95", TestKafkaOutputMessage.parseFrom(records.get(2).value()).getOrderId());
    }

    @Test
    public void shouldAutoCreateOutputTopicOnInit() throws Exception {
        String topic = uniqueTopic("autocreate");
        Map<String, String> config = baseConfig(topic, DEFAULT_MAPPING, true);
        config.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "2");
        config.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "1");
        config.put("SINK_KAFKA_TOPIC_RETENTION_HR", "24");

        KafkaSinkFactory factory = new KafkaSinkFactory(config, statsDReporter);
        factory.init();
        sink = factory.create();

        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            Map<String, TopicDescription> descriptions = adminClient.describeTopics(Collections.singletonList(topic))
                    .all().get(30, TimeUnit.SECONDS);
            TopicDescription description = descriptions.get(topic);
            assertNotNull(description);
            assertEquals(2, description.partitions().size());
            assertEquals(1, description.partitions().get(0).replicas().size());
        }
    }

    @Test
    public void shouldReportDeserializationErrorAndStillProduceValidMessages() throws Exception {
        String topic = uniqueTopic("mixed-errors");
        sink = createSink(topic, DEFAULT_MAPPING, true);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, sourceMessage.toByteArray()));
        messages.add(new Message(null, "not-valid-protobuf".getBytes()));
        messages.add(new Message(null, sourceMessage.toBuilder().setOrderNumber(100L).build().toByteArray()));

        SinkResponse response = sink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(1, response.getErrors().size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(1L).getErrorType());
        assertNull(response.getErrorsFor(0L));
        assertNull(response.getErrorsFor(2L));

        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(topic, 2);
        assertEquals(2, records.size());
        assertEquals("93", TestKafkaOutputMessage.parseFrom(records.get(0).value()).getOrderId());
        assertEquals("100", TestKafkaOutputMessage.parseFrom(records.get(1).value()).getOrderId());
    }

    @Test
    public void shouldReportMappingEvaluationFailuresAsInvalidMessageErrors() throws Exception {
        String topic = uniqueTopic("mapping-error");
        String mapping = "{\"order_id\": \"string(source.order_list[0])\", \"user_id\": \"source.account_go_id\"}";
        sink = createSink(topic, mapping, true);

        TestKafkaSourceMessage emptyOrderList = TestKafkaSourceMessage.newBuilder()
                .setOrderNumber(1L)
                .setAccountGoId("user-x")
                .build();

        SinkResponse response = sink.pushToSink(Collections.singletonList(
                new Message(null, emptyOrderList.toByteArray())));

        assertTrue(response.hasErrors());
        assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrorsFor(0L).getErrorType());
        assertTrue(consumeRecords(topic, 0, 3_000).isEmpty());
    }

    @Test
    public void shouldProduceWithoutKeyWhenKeyProtoIsNotConfigured() throws Exception {
        String topic = uniqueTopic("no-key");
        String mapping = "{\"order_id\": \"string(source.order_number)\", \"user_id\": \"source.account_go_id\"}";
        sink = createSink(topic, mapping, false);

        SinkResponse response = sink.pushToSink(Collections.singletonList(
                new Message(null, sourceMessage.toByteArray())));

        assertFalse(response.hasErrors());
        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(topic, 1);
        assertEquals(1, records.size());
        assertNull(records.get(0).key());
        assertEquals("93", TestKafkaOutputMessage.parseFrom(records.get(0).value()).getOrderId());
        assertEquals("user-1", TestKafkaOutputMessage.parseFrom(records.get(0).value()).getUserId());
    }

    @Test
    public void shouldReuseExistingTopicWithoutFailingInit() throws Exception {
        String topic = uniqueTopic("existing");
        Map<String, String> firstConfig = baseConfig(topic, DEFAULT_MAPPING, true);
        firstConfig.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "1");
        firstConfig.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "1");
        KafkaSinkFactory firstFactory = new KafkaSinkFactory(firstConfig, statsDReporter);
        firstFactory.init();
        Sink firstSink = firstFactory.create();
        firstSink.close();

        Map<String, String> secondConfig = baseConfig(topic, DEFAULT_MAPPING, true);
        secondConfig.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "5");
        secondConfig.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "1");
        KafkaSinkFactory secondFactory = new KafkaSinkFactory(secondConfig, statsDReporter);
        secondFactory.init();
        sink = secondFactory.create();

        SinkResponse response = sink.pushToSink(Collections.singletonList(
                new Message(null, sourceMessage.toByteArray())));
        assertFalse(response.hasErrors());
        assertEquals(1, consumeRecords(topic, 1).size());

        try (AdminClient adminClient = AdminClient.create(adminProperties())) {
            TopicDescription description = adminClient.describeTopics(Collections.singletonList(topic))
                    .all().get(30, TimeUnit.SECONDS).get(topic);
            assertEquals(1, description.partitions().size());
        }
    }

    private Sink createSink(String topic, String mapping, boolean withKeyProto) throws Exception {
        Map<String, String> config = baseConfig(topic, mapping, withKeyProto);
        KafkaSinkFactory factory = new KafkaSinkFactory(config, statsDReporter);
        factory.init();
        return factory.create();
    }

    private Map<String, String> baseConfig(String topic, String mapping, boolean withKeyProto) {
        Map<String, String> config = new HashMap<>();
        config.put("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", SOURCE_PROTO);
        config.put("SINK_KAFKA_BROKERS", KAFKA.getBootstrapServers());
        config.put("SINK_KAFKA_TOPIC", topic);
        config.put("SINK_KAFKA_PROTO_MESSAGE", OUTPUT_MESSAGE_PROTO);
        if (withKeyProto) {
            config.put("SINK_KAFKA_PROTO_KEY", OUTPUT_KEY_PROTO);
        }
        config.put("SINK_KAFKA_PROTO_MAPPING", mapping);
        config.put("SINK_KAFKA_TOPIC_PARTITION_COUNT", "1");
        config.put("SINK_KAFKA_TOPIC_REPLICATION_FACTOR", "1");
        config.put("SINK_KAFKA_LINGER_MS", "0");
        config.put("SINK_KAFKA_ACKS", "all");
        return config;
    }

    private Properties adminProperties() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        return properties;
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeRecords(String topic, int expectedCount) {
        return consumeRecords(topic, expectedCount, 30_000);
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeRecords(String topic, int expectedCount, long timeoutMs) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-sink-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            List<ConsumerRecord<byte[], byte[]>> collected = new ArrayList<>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (expectedCount > 0 && collected.size() >= expectedCount) {
                    break;
                }
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : polled) {
                    collected.add(record);
                }
                if (expectedCount == 0 && !polled.isEmpty()) {
                    break;
                }
            }
            return collected;
        }
    }

    private static String uniqueTopic(String prefix) {
        return "depot-kafka-sink-it-" + prefix + "-" + UUID.randomUUID().toString().replace("-", "");
    }
}
