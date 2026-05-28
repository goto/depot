package com.gotocompany.depot.kafka;

import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.stencil.client.StencilClient;
import dev.cel.runtime.CelEvaluationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTest {

    private static final String TOPIC = "output-topic";
    private static final String SOURCE_CLASS = "com.gotocompany.depot.kafka.test.KafkaSourceMessage";

    @Mock
    private KafkaProducer<byte[], byte[]> producer;
    @Mock
    private StencilClient sourceStencilClient;
    @Mock
    private ProtoMappingEngine mappingEngine;
    @Mock
    private Instrumentation instrumentation;

    @Test
    public void shouldPushMappedRecordToKafka() throws Exception {
        byte[] keyBytes = new byte[] {1};
        byte[] valueBytes = new byte[] {2, 3};
        DynamicMessage sinkKey = mock(DynamicMessage.class);
        DynamicMessage sinkMessage = mock(DynamicMessage.class);
        when(sinkKey.toByteArray()).thenReturn(keyBytes);
        when(sinkMessage.toByteArray()).thenReturn(valueBytes);

        DynamicMessage sourceMessage = mock(DynamicMessage.class);
        when(sourceStencilClient.parse(SOURCE_CLASS, new byte[] {9})).thenReturn(sourceMessage);
        when(mappingEngine.map(sourceMessage)).thenReturn(
                new ProtoMappingEngine.MappedRecord(sinkKey, sinkMessage));

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> future = mock(Future.class);
        when(future.get()).thenReturn(mock(RecordMetadata.class));
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        KafkaSink sink = new KafkaSink(
                producer, TOPIC, sourceStencilClient, SOURCE_CLASS, mappingEngine, instrumentation);

        SinkResponse response = sink.pushToSink(
                Collections.singletonList(new Message(null, new byte[] {9})));

        Assert.assertFalse(response.hasErrors());
        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture());
        Assert.assertEquals(TOPIC, captor.getValue().topic());
        Assert.assertArrayEquals(keyBytes, (byte[]) captor.getValue().key());
        Assert.assertArrayEquals(valueBytes, (byte[]) captor.getValue().value());
    }

    @Test
    public void shouldReportDeserializationErrorForEmptyPayload() throws Exception {
        KafkaSink sink = new KafkaSink(
                producer, TOPIC, sourceStencilClient, SOURCE_CLASS, mappingEngine, instrumentation);

        SinkResponse response = sink.pushToSink(
                Collections.singletonList(new Message(null, new byte[0])));

        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(0).getErrorType());
        Assert.assertTrue(response.getErrorsFor(0).getException() instanceof EmptyMessageException);
    }

    @Test
    public void shouldReportInvalidMessageErrorForCelFailure() throws Exception {
        DynamicMessage sourceMessage = mock(DynamicMessage.class);
        when(sourceStencilClient.parse(SOURCE_CLASS, new byte[] {1})).thenReturn(sourceMessage);
        when(mappingEngine.map(sourceMessage)).thenThrow(new CelEvaluationException("eval failed"));

        KafkaSink sink = new KafkaSink(
                producer, TOPIC, sourceStencilClient, SOURCE_CLASS, mappingEngine, instrumentation);

        SinkResponse response = sink.pushToSink(
                Collections.singletonList(new Message(null, new byte[] {1})));

        Assert.assertEquals(ErrorType.INVALID_MESSAGE_ERROR, response.getErrorsFor(0).getErrorType());
    }

    @Test
    public void shouldReportSinkUnknownErrorWhenProducerFails() throws Exception {
        DynamicMessage sourceMessage = mock(DynamicMessage.class);
        when(sourceStencilClient.parse(SOURCE_CLASS, new byte[] {1})).thenReturn(sourceMessage);
        when(mappingEngine.map(sourceMessage)).thenReturn(
                new ProtoMappingEngine.MappedRecord(
                        mock(DynamicMessage.class), mock(DynamicMessage.class)));

        @SuppressWarnings("unchecked")
        Future<RecordMetadata> future = mock(Future.class);
        when(future.get()).thenThrow(new ExecutionException(new IOException("broker down")));
        when(producer.send(any(ProducerRecord.class))).thenReturn(future);

        KafkaSink sink = new KafkaSink(
                producer, TOPIC, sourceStencilClient, SOURCE_CLASS, mappingEngine, instrumentation);

        SinkResponse response = sink.pushToSink(
                Collections.singletonList(new Message(null, new byte[] {1})));

        Assert.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, response.getErrorsFor(0).getErrorType());
    }
}
