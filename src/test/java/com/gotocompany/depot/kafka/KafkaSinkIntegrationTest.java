package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.TestBookingLogKey;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestBookingStatus;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.TestServiceType;
import com.gotocompany.depot.config.KafkaSinkConfig;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.ClassLoadStencilClient;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration test for the full KafkaSink pushToSink flow:
 * source bytes → DynamicMessage → CEL mapping → serialization → Kafka produce.
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkIntegrationTest {

    private static final String SOURCE_PROTO = "com.gotocompany.depot.TestBookingLogMessage";
    private static final String SINK_MESSAGE_PROTO = "com.gotocompany.depot.TestBookingLogMessage";
    private static final String SINK_KEY_PROTO = "com.gotocompany.depot.TestBookingLogKey";
    private static final String TOPIC = "output-topic";

    @Mock
    private KafkaSinkConfig config;

    @Mock
    private Instrumentation instrumentation;

    @Mock
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    private StencilClient stencilClient;
    private KafkaProtoMappingEngine mappingEngine;
    private KafkaSinkProducer sinkProducer;
    private KafkaSink kafkaSink;
    private byte[] sourceMessageBytes;
    private byte[] sourceKeyBytes;

    @Before
    public void setUp() throws InvalidProtocolBufferException {
        stencilClient = Mockito.mock(ClassLoadStencilClient.class, CALLS_REAL_METHODS);

        // Build a test source message
        TestBookingLogMessage bookingMessage = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-12345")
                .setOrderUrl("http://example.com/orders/12345")
                .setCustomerId("CUST-001")
                .setDriverId("DRV-001")
                .setAmountPaidByCash(150.5f)
                .setCancelReasonId(5)
                .setServiceType(TestServiceType.Enum.GO_RIDE)
                .setStatus(TestBookingStatus.Enum.COMPLETED)
                .setDriverPickupLocation(TestLocation.newBuilder()
                        .setName("Pickup Point")
                        .setLatitude(6.2088)
                        .setLongitude(106.8456)
                        .build())
                .build();
        sourceMessageBytes = bookingMessage.toByteArray();

        TestBookingLogKey key = TestBookingLogKey.newBuilder()
                .setOrderNumber("ORD-12345")
                .setOrderUrl("http://example.com/orders/12345")
                .build();
        sourceKeyBytes = key.toByteArray();

        // Configure
        when(config.getSinkKafkaTopic()).thenReturn(TOPIC);
        when(config.getSinkConnectorSchemaProtoMessageClass()).thenReturn(SOURCE_PROTO);

        // Set up Kafka producer mock
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(TOPIC, 0), 0, 0, 0, 0L, 0, 0);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        when(kafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        sinkProducer = new KafkaSinkProducer(kafkaProducer, TOPIC, instrumentation);
    }

    @Test
    public void shouldPushMessageWithDirectMapping() {
        String mapping = "{\"order_number\": \"source.order_number\", \"order_url\": \"source.order_url\"}";
        when(config.getSinkKafkaProtoMessage()).thenReturn(SINK_MESSAGE_PROTO);
        when(config.getSinkKafkaProtoKey()).thenReturn(SINK_KEY_PROTO);
        when(config.getSinkKafkaProtoMapping()).thenReturn(mapping);

        mappingEngine = new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
        kafkaSink = new KafkaSink(config, mappingEngine, sinkProducer, stencilClient, instrumentation);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));

        SinkResponse response = kafkaSink.pushToSink(messages);

        Assert.assertFalse(response.hasErrors());

        // Verify the produced record
        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());
        ProducerRecord<byte[], byte[]> record = captor.getValue();
        Assert.assertEquals(TOPIC, record.topic());
        Assert.assertNotNull(record.value());
        Assert.assertNotNull(record.key());

        // Verify the produced value can be parsed back as the target proto
        try {
            DynamicMessage parsedValue = DynamicMessage.parseFrom(
                    stencilClient.get(SINK_MESSAGE_PROTO), record.value());
            Assert.assertEquals("ORD-12345",
                    parsedValue.getField(parsedValue.getDescriptorForType().findFieldByName("order_number")));
            Assert.assertEquals("http://example.com/orders/12345",
                    parsedValue.getField(parsedValue.getDescriptorForType().findFieldByName("order_url")));
        } catch (InvalidProtocolBufferException e) {
            Assert.fail("Produced value should be valid protobuf: " + e.getMessage());
        }
    }

    @Test
    public void shouldPushMultipleMessages() {
        String mapping = "{\"order_number\": \"source.order_number\"}";
        when(config.getSinkKafkaProtoMessage()).thenReturn(SINK_MESSAGE_PROTO);
        when(config.getSinkKafkaProtoKey()).thenReturn(SINK_KEY_PROTO);
        when(config.getSinkKafkaProtoMapping()).thenReturn(mapping);

        mappingEngine = new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
        kafkaSink = new KafkaSink(config, mappingEngine, sinkProducer, stencilClient, instrumentation);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));

        SinkResponse response = kafkaSink.pushToSink(messages);

        Assert.assertFalse(response.hasErrors());
        verify(kafkaProducer, times(3)).send(any(ProducerRecord.class));
    }

    @Test
    public void shouldHandleInvalidSourceBytes() {
        String mapping = "{\"order_number\": \"source.order_number\"}";
        when(config.getSinkKafkaProtoMessage()).thenReturn(SINK_MESSAGE_PROTO);
        when(config.getSinkKafkaProtoKey()).thenReturn(SINK_KEY_PROTO);
        when(config.getSinkKafkaProtoMapping()).thenReturn(mapping);

        mappingEngine = new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
        kafkaSink = new KafkaSink(config, mappingEngine, sinkProducer, stencilClient, instrumentation);

        List<Message> messages = new ArrayList<>();
        // Add a valid message
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));
        // Add a message with invalid protobuf bytes
        messages.add(new Message(sourceKeyBytes, "not-valid-protobuf".getBytes()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        // First message succeeds, second fails during parse/mapping
        verify(kafkaProducer, times(1)).send(any(ProducerRecord.class));
    }

    @Test
    public void shouldReportProducerErrors() {
        String mapping = "{\"order_number\": \"source.order_number\"}";
        when(config.getSinkKafkaProtoMessage()).thenReturn(SINK_MESSAGE_PROTO);
        when(config.getSinkKafkaProtoKey()).thenReturn(SINK_KEY_PROTO);
        when(config.getSinkKafkaProtoMapping()).thenReturn(mapping);

        // Make the producer return a failed future
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(kafkaProducer.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        mappingEngine = new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
        kafkaSink = new KafkaSink(config, mappingEngine, sinkProducer, stencilClient, instrumentation);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));

        SinkResponse response = kafkaSink.pushToSink(messages);

        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR,
                response.getErrors().get(0L).getErrorType());
    }

    @Test
    public void shouldPushWithCelTransformation() {
        String mapping = "{\"order_number\": \"'PREFIX-' + source.order_number\", \"cancel_reason_id\": \"source.cancel_reason_id\"}";
        when(config.getSinkKafkaProtoMessage()).thenReturn(SINK_MESSAGE_PROTO);
        when(config.getSinkKafkaProtoKey()).thenReturn("");
        when(config.getSinkKafkaProtoMapping()).thenReturn(mapping);

        mappingEngine = new KafkaProtoMappingEngine(config, stencilClient, stencilClient, instrumentation);
        kafkaSink = new KafkaSink(config, mappingEngine, sinkProducer, stencilClient, instrumentation);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(sourceKeyBytes, sourceMessageBytes));

        SinkResponse response = kafkaSink.pushToSink(messages);

        Assert.assertFalse(response.hasErrors());

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());
        ProducerRecord<byte[], byte[]> record = captor.getValue();

        // When no key proto is configured, the original log key bytes should be used
        Assert.assertArrayEquals(sourceKeyBytes, record.key());

        // Verify transformed value
        try {
            DynamicMessage parsedValue = DynamicMessage.parseFrom(
                    stencilClient.get(SINK_MESSAGE_PROTO), record.value());
            Assert.assertEquals("PREFIX-ORD-12345",
                    parsedValue.getField(parsedValue.getDescriptorForType().findFieldByName("order_number")));
            Assert.assertEquals(5,
                    parsedValue.getField(parsedValue.getDescriptorForType().findFieldByName("cancel_reason_id")));
        } catch (InvalidProtocolBufferException e) {
            Assert.fail("Produced value should be valid protobuf: " + e.getMessage());
        }
    }
}
