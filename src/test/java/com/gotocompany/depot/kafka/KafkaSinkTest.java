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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
}
