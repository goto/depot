package com.gotocompany.depot.kafka;

import com.gotocompany.depot.metrics.Instrumentation;
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
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkProducerTest {

    @Mock
    private KafkaProducer<byte[], byte[]> kafkaProducer;

    @Mock
    private Instrumentation instrumentation;

    private KafkaSinkProducer sinkProducer;

    @Before
    public void setUp() {
        sinkProducer = new KafkaSinkProducer(kafkaProducer, "output-topic", instrumentation);
    }

    @Test
    public void shouldProduceMessageToTopic() {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("output-topic", 0), 0, 0, 0, 0L, 0, 0);
        CompletableFuture<RecordMetadata> future = CompletableFuture.completedFuture(metadata);
        when(kafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);

        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        Future<RecordMetadata> result = sinkProducer.produce(key, value);

        Assert.assertNotNull(result);

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());
        ProducerRecord<byte[], byte[]> record = captor.getValue();
        Assert.assertEquals("output-topic", record.topic());
        Assert.assertArrayEquals(key, record.key());
        Assert.assertArrayEquals(value, record.value());
    }

    @Test
    public void shouldFlushProducer() {
        sinkProducer.flush();
        verify(kafkaProducer).flush();
    }

    @Test
    public void shouldCloseProducer() throws Exception {
        sinkProducer.close();
        verify(kafkaProducer).close();
    }
}
