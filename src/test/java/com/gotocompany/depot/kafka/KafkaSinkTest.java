package com.gotocompany.depot.kafka;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.TestKey;
import com.gotocompany.depot.TestMessage;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.kafka.mapping.ProtoMappingFunction;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import dev.cel.runtime.CelEvaluationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TimeoutException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTest {

    @Mock
    private KafkaProducer<byte[], byte[]> producer;
    @Mock
    private ProtoMappingFunction mappingFunction;
    @Mock
    private Instrumentation instrumentation;

    private Descriptors.Descriptor sourceMessageDescriptor;
    private Descriptors.Descriptor sourceKeyDescriptor;
    private KafkaSink kafkaSink;

    @Before
    public void setUp() {
        sourceMessageDescriptor = TestMessage.getDescriptor();
        sourceKeyDescriptor = TestKey.getDescriptor();
        kafkaSink = new KafkaSink(producer, mappingFunction, sourceMessageDescriptor, sourceKeyDescriptor,
                "output-topic", instrumentation);
    }

    @Test
    public void shouldPushMessagesSuccessfully() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("http://test.com").build();
        TestKey testKey = TestKey.newBuilder().setOrderNumber("123").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        DynamicMessage outputKey = DynamicMessage.newBuilder(sourceKeyDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, outputKey);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0));
        when(producer.send(any())).thenReturn(future);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(testKey.toByteArray(), testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertFalse(response.hasErrors());
        verify(producer, times(1)).send(any());
    }

    @Test
    public void shouldReportMappingErrors() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("123").build();
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        when(mappingFunction.map(any(DynamicMessage.class))).thenThrow(
                new CelEvaluationException("mapping failed"));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(1, response.getErrors().size());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldReportProducerErrors() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("123").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker down"));
        when(producer.send(any())).thenReturn(failedFuture);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldHandleMultipleMessagesWithMixedResults() throws Exception {
        TestMessage testMsg1 = TestMessage.newBuilder().setOrderNumber("1").build();
        TestMessage testMsg2 = TestMessage.newBuilder().setOrderNumber("2").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class)))
                .thenReturn(mapped)
                .thenThrow(new CelEvaluationException("fail on second"));

        Future<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0));
        when(producer.send(any())).thenReturn(future);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg1.toByteArray()));
        messages.add(new Message(null, testMsg2.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(1, response.getErrors().size());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(1).getErrorType());
    }

    @Test
    public void shouldReturnEmptyResponseForEmptyMessageList() {
        List<Message> messages = new ArrayList<>();

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertFalse(response.hasErrors());
        verify(producer, never()).send(any());
    }

    @Test
    public void shouldReportRetriableErrorForNetworkException() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("123").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new NetworkException("connection reset")));
        when(producer.send(any())).thenReturn(failedFuture);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldReportRetriableErrorForTimeoutException() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("456").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new TimeoutException("request timed out")));
        when(producer.send(any())).thenReturn(failedFuture);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldReportRetriableErrorForNotLeaderException() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("789").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        CompletableFuture<RecordMetadata> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new ExecutionException(new NotLeaderOrFollowerException("not leader")));
        when(producer.send(any())).thenReturn(failedFuture);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldHandleNullKeyBytesInMessage() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("no-key").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0));
        when(producer.send(any())).thenReturn(future);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertFalse(response.hasErrors());
        verify(producer, times(1)).send(any());
    }

    @Test
    public void shouldCloseFlushAndCloseProducer() throws IOException {
        kafkaSink.close();

        verify(producer, times(1)).flush();
        verify(producer, times(1)).close();
    }

    @Test
    public void shouldPushMultipleMessagesSuccessfully() throws Exception {
        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        DynamicMessage outputKey = DynamicMessage.newBuilder(sourceKeyDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, outputKey);

        when(mappingFunction.map(any(DynamicMessage.class))).thenReturn(mapped);
        Future<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0));
        when(producer.send(any())).thenReturn(future);

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TestMessage msg = TestMessage.newBuilder().setOrderNumber("order-" + i).build();
            TestKey key = TestKey.newBuilder().setOrderNumber("key-" + i).build();
            messages.add(new Message(key.toByteArray(), msg.toByteArray()));
        }

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertFalse(response.hasErrors());
        verify(producer, times(5)).send(any());
    }

    @Test
    public void shouldContinueProcessingAfterErrorInMiddle() throws Exception {
        TestMessage msg1 = TestMessage.newBuilder().setOrderNumber("1").build();
        TestMessage msg2 = TestMessage.newBuilder().setOrderNumber("2").build();
        TestMessage msg3 = TestMessage.newBuilder().setOrderNumber("3").build();

        DynamicMessage outputMsg = DynamicMessage.newBuilder(sourceMessageDescriptor).build();
        ProtoMappingFunction.MappedMessages mapped = new ProtoMappingFunction.MappedMessages(outputMsg, null);

        when(mappingFunction.map(any(DynamicMessage.class)))
                .thenReturn(mapped)
                .thenThrow(new CelEvaluationException("fail"))
                .thenReturn(mapped);

        Future<RecordMetadata> future = CompletableFuture.completedFuture(
                new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0));
        when(producer.send(any())).thenReturn(future);

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, msg1.toByteArray()));
        messages.add(new Message(null, msg2.toByteArray()));
        messages.add(new Message(null, msg3.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(1, response.getErrors().size());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(1).getErrorType());
        verify(producer, times(2)).send(any());
    }

    @Test
    public void shouldReportAllErrorsWhenAllMessagesFail() throws Exception {
        when(mappingFunction.map(any(DynamicMessage.class)))
                .thenThrow(new CelEvaluationException("fail1"))
                .thenThrow(new CelEvaluationException("fail2"))
                .thenThrow(new CelEvaluationException("fail3"));

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            TestMessage msg = TestMessage.newBuilder().setOrderNumber("order-" + i).build();
            messages.add(new Message(null, msg.toByteArray()));
        }

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(3, response.getErrors().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(i).getErrorType());
        }
        verify(producer, never()).send(any());
    }

    @Test
    public void shouldReportDeserializationErrorForInvalidPayload() throws Exception {
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, "invalid-protobuf-payload".getBytes()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(1, response.getErrors().size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(0).getErrorType());
        verify(producer, never()).send(any());
    }

    @Test
    public void shouldHandleMultipleDeserializationErrors() {
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, "bad-data-1".getBytes()));
        messages.add(new Message(null, "bad-data-2".getBytes()));
        messages.add(new Message(null, "bad-data-3".getBytes()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(3, response.getErrors().size());
        for (int i = 0; i < 3; i++) {
            assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(i).getErrorType());
        }
    }

    @Test
    public void shouldHandleMixedDeserializationAndMappingErrors() throws Exception {
        TestMessage testMsg = TestMessage.newBuilder().setOrderNumber("valid").build();

        when(mappingFunction.map(any(DynamicMessage.class)))
                .thenThrow(new CelEvaluationException("mapping-error"));

        List<Message> messages = new ArrayList<>();
        messages.add(new Message(null, "bad-data".getBytes()));
        messages.add(new Message(null, testMsg.toByteArray()));

        SinkResponse response = kafkaSink.pushToSink(messages);

        assertTrue(response.hasErrors());
        assertEquals(2, response.getErrors().size());
        assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(0).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(1).getErrorType());
    }


}
