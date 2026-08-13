package com.gotocompany.depot.kafka.client;

import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.response.KafkaProduceResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaProducerClientTest {

    @Mock
    private Producer<byte[], byte[]> producer;
    @Mock
    private Instrumentation instrumentation;
    private KafkaProducerClient client;

    @Before
    public void setup() {
        client = new KafkaProducerClient(producer, "output-topic", instrumentation);
    }

    private CompletableFuture<RecordMetadata> successFuture() {
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<RecordMetadata> failedFuture(Throwable error) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        future.completeExceptionally(error);
        return future;
    }

    @Test
    public void shouldSendRecordsToConfiguredTopic() {
        when(producer.send(any())).thenReturn(successFuture());
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, new byte[]{1}, new byte[]{2}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{3}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertEquals(2, responses.size());
        assertFalse(responses.get(0).isFailed());
        assertFalse(responses.get(1).isFailed());
        ArgumentCaptor<ProducerRecord<byte[], byte[]>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer, Mockito.times(2)).send(captor.capture());
        verify(producer, Mockito.times(1)).flush();
        assertEquals("output-topic", captor.getAllValues().get(0).topic());
        assertArrayEquals(new byte[]{1}, captor.getAllValues().get(0).key());
        assertArrayEquals(new byte[]{2}, captor.getAllValues().get(0).value());
        assertEquals("output-topic", captor.getAllValues().get(1).topic());
        assertArrayEquals(null, captor.getAllValues().get(1).key());
        assertArrayEquals(new byte[]{3}, captor.getAllValues().get(1).value());
    }

    @Test
    public void shouldReturnFailureResponseForFailedFutures() {
        when(producer.send(any()))
                .thenReturn(successFuture())
                .thenReturn(failedFuture(new TimeoutException("broker timeout")));
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertFalse(responses.get(0).isFailed());
        assertTrue(responses.get(1).isFailed());
        assertTrue(responses.get(1).getError() instanceof TimeoutException);
        assertEquals("broker timeout", responses.get(1).getMessage());
    }

    @Test
    public void shouldReturnFailureResponseWhenSendThrows() {
        when(producer.send(any()))
                .thenReturn(successFuture())
                .thenThrow(new KafkaException("producer failure"))
                .thenReturn(successFuture());
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"),
                KafkaRecord.validRecord(2L, null, new byte[]{3}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertFalse(responses.get(0).isFailed());
        assertTrue(responses.get(1).isFailed());
        assertEquals("producer failure", responses.get(1).getMessage());
        assertFalse(responses.get(2).isFailed());
    }

    @Test
    public void shouldReturnFailureResponseWhenFutureIsInterrupted() throws Exception {
        Future<RecordMetadata> interruptedFuture = Mockito.mock(Future.class);
        when(interruptedFuture.get()).thenThrow(new InterruptedException("interrupted"));
        when(producer.send(any())).thenReturn(interruptedFuture);
        List<KafkaRecord> records = Arrays.asList(KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertTrue(responses.get(0).isFailed());
        assertTrue(responses.get(0).getError() instanceof InterruptedException);
        assertTrue(Thread.interrupted());
    }

    @Test
    public void shouldCloseProducer() {
        client.close();
        verify(producer, Mockito.times(1)).close();
    }

    @Test
    public void shouldReturnEmptyResponsesForEmptyRecords() {
        List<KafkaProduceResponse> responses = client.send(Collections.emptyList());
        assertTrue(responses.isEmpty());
        verify(producer, Mockito.times(1)).flush();
        verify(producer, never()).send(any());
    }

    @Test
    public void shouldReturnFailureResponsesWhenAllSendsThrow() {
        when(producer.send(any())).thenThrow(new IllegalStateException("producer already closed"));
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertTrue(responses.get(0).isFailed());
        assertTrue(responses.get(1).isFailed());
        assertTrue(responses.get(0).getError() instanceof IllegalStateException);
        assertEquals("producer already closed", responses.get(1).getMessage());
    }

    @Test
    public void shouldPropagateFlushFailures() {
        when(producer.send(any())).thenReturn(successFuture());
        Mockito.doThrow(new KafkaException("flush failed")).when(producer).flush();
        List<KafkaRecord> records = Arrays.asList(KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"));
        KafkaException exception = org.junit.Assert.assertThrows(KafkaException.class, () -> client.send(records));
        assertEquals("flush failed", exception.getMessage());
    }

    @Test
    public void shouldReturnFailureResponseForNonKafkaExecutionFailures() {
        when(producer.send(any())).thenReturn(failedFuture(new RuntimeException("unexpected failure")));
        List<KafkaRecord> records = Arrays.asList(KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"));
        List<KafkaProduceResponse> responses = client.send(records);
        assertTrue(responses.get(0).isFailed());
        assertTrue(responses.get(0).getError() instanceof RuntimeException);
        assertEquals("unexpected failure", responses.get(0).getMessage());
    }
}
