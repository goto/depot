package com.gotocompany.depot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaProducerWrapperTest {

    @Mock
    private KafkaProducer<byte[], byte[]> mockProducer;

    private KafkaProducerWrapper wrapper;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        wrapper = new KafkaProducerWrapper(mockProducer, "test-topic");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSendRecordWithKeyAndValue() {
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("test-topic", 0), 0, 0, 0, 0L, 0, 0);
        when(mockProducer.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        Future<RecordMetadata> future = wrapper.send(key, value);

        assertNotNull(future);

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(mockProducer).send(captor.capture());

        ProducerRecord<byte[], byte[]> captured = captor.getValue();
        assertEquals("test-topic", captured.topic());
        assertEquals("key", new String(captured.key()));
        assertEquals("value", new String(captured.value()));
    }

    @Test
    public void shouldFlushProducer() {
        wrapper.flush();
        verify(mockProducer).flush();
    }

    @Test
    public void shouldCloseProducer() throws Exception {
        wrapper.close();
        verify(mockProducer).close();
    }
}
