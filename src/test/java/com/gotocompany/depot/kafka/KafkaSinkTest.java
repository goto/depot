package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.message.Message;
import com.gotocompany.stencil.StencilClientFactory;
import com.gotocompany.stencil.client.StencilClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class KafkaSinkTest {

    @Mock
    private KafkaProducer<byte[], byte[]> mockProducer;

    private KafkaSink kafkaSink;
    private StencilClient stencilClient;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        stencilClient = StencilClientFactory.getClient();

        Descriptors.Descriptor sourceDesc = stencilClient.get(
                "com.gotocompany.depot.TestBookingLogMessage");
        Descriptors.Descriptor sinkDesc = stencilClient.get(
                "com.gotocompany.depot.TestMessage");

        String mapping = "{"
                + "\"order_number\": \"source.order_number\","
                + "\"order_url\": \"source.order_url\""
                + "}";

        ProtoMapper valueMapper = new ProtoMapper(sourceDesc, sinkDesc, mapping);
        KafkaMessageParser messageParser = new KafkaMessageParser(stencilClient,
                "com.gotocompany.depot.TestBookingLogMessage");
        KafkaMessageSerializer serializer = new KafkaMessageSerializer();
        KafkaProducerWrapper producerWrapper = new KafkaProducerWrapper(mockProducer, "output-topic");

        kafkaSink = new KafkaSink(messageParser, valueMapper, null, serializer, producerWrapper);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPushSingleMessageSuccessfully() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0);
        when(mockProducer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-1")
                .setOrderUrl("http://example.com")
                .build();

        List<Message> messages = Collections.singletonList(
                new Message(null, source.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertFalse(response.hasErrors());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPushMultipleMessagesSuccessfully() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0);
        when(mockProducer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                    .setOrderNumber("ORD-" + i)
                    .setOrderUrl("http://example.com/" + i)
                    .build();
            messages.add(new Message(null, source.toByteArray()));
        }

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertFalse(response.hasErrors());
    }

    @Test
    public void shouldReportDeserializationErrorForInvalidMessage() {
        List<Message> messages = Collections.singletonList(
                new Message(null, "invalid-bytes".getBytes()));

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertNotNull(response.getErrorsFor(0));
    }

    @Test
    public void shouldReportErrorForNullMessage() {
        List<Message> messages = Collections.singletonList(
                new Message(null, null));

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertNotNull(response.getErrorsFor(0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReportProducerErrorPerMessage() {
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Broker unavailable"));
        when(mockProducer.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        TestBookingLogMessage source = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-1")
                .build();
        List<Message> messages = Collections.singletonList(
                new Message(null, source.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR,
                response.getErrorsFor(0).getErrorType());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldContinueProcessingAfterErrorInMiddle() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0);
        when(mockProducer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        TestBookingLogMessage validMsg = TestBookingLogMessage.newBuilder()
                .setOrderNumber("ORD-1")
                .build();

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, validMsg.toByteArray()));
        messages.add(new Message(null, "invalid".getBytes()));
        messages.add(new Message(null, validMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertNull(response.getErrorsFor(0));
        Assert.assertNotNull(response.getErrorsFor(1));
        Assert.assertNull(response.getErrorsFor(2));
    }
}
