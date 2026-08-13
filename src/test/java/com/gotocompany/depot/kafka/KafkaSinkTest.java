package com.gotocompany.depot.kafka;

import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.kafka.client.KafkaSinkClient;
import com.gotocompany.depot.kafka.parser.KafkaRecordParser;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.kafka.response.KafkaProduceResponse;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTest {

    @Mock
    private KafkaSinkClient kafkaSinkClient;
    @Mock
    private KafkaRecordParser recordParser;
    @Mock
    private KafkaSinkMetrics metrics;
    @Mock
    private Instrumentation instrumentation;
    private KafkaSink kafkaSink;

    @Before
    public void setup() {
        kafkaSink = new KafkaSink(kafkaSinkClient, recordParser, metrics, instrumentation);
    }

    @Test
    public void shouldPushAllValidRecordsToKafka() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenReturn(Arrays.asList(
                KafkaProduceResponse.success(),
                KafkaProduceResponse.success()));
        when(metrics.getKafkaProduceBatchSizeMetric()).thenReturn("batch_size");
        when(metrics.getKafkaProduceLatencyMetric()).thenReturn("latency");
        when(metrics.getKafkaMessagesProducedTotalMetric()).thenReturn("produced");
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertFalse(response.hasErrors());
        verify(instrumentation, Mockito.times(1)).captureHistogram("batch_size", 2L);
        verify(instrumentation, Mockito.times(1)).captureCount("produced", 2L);
        verify(instrumentation, Mockito.times(1)).captureDurationSince(Mockito.eq("latency"), Mockito.any(Instant.class));
        verify(instrumentation, Mockito.times(1))
                .logInfo("Pushed {} valid records to Kafka, {} failed parsing, {} failed producing", 2, 0, 0);
    }

    @Test
    public void shouldCaptureRecordParseErrorMetricsByErrorType() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.invalidRecord(0L, new ErrorInfo(new IOException("parse error"), ErrorType.DESERIALIZATION_ERROR), "{}"),
                KafkaRecord.invalidRecord(1L, new ErrorInfo(new RuntimeException("boom"), ErrorType.SINK_UNKNOWN_ERROR), "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(metrics.getKafkaRecordParseErrorsTotalMetric()).thenReturn("parse_errors");
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        verify(instrumentation, Mockito.times(1)).incrementCounter("parse_errors", "error_type=DESERIALIZATION_ERROR");
        verify(instrumentation, Mockito.times(1)).incrementCounter("parse_errors", "error_type=SINK_UNKNOWN_ERROR");
        verify(kafkaSinkClient, never()).send(Mockito.anyList());
        verify(instrumentation, Mockito.times(1)).logInfo("No valid records to push to Kafka, {} records failed parsing", 2);
    }

    @Test
    public void shouldReportInvalidRecordsAsErrors() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.invalidRecord(0L, new ErrorInfo(new IOException("parse error"), ErrorType.DESERIALIZATION_ERROR), "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        List<KafkaRecord> validRecords = records.stream().filter(KafkaRecord::isValid).collect(Collectors.toList());
        when(kafkaSinkClient.send(validRecords)).thenReturn(Arrays.asList(KafkaProduceResponse.success()));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(1, response.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(0L).getErrorType());
    }

    @Test
    public void shouldNotSendWhenAllRecordsAreInvalid() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.invalidRecord(0L, new ErrorInfo(new IOException("parse error"), ErrorType.DESERIALIZATION_ERROR), "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        verify(kafkaSinkClient, never()).send(Mockito.anyList());
    }

    @Test
    public void shouldReportProduceFailuresAsErrors() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenReturn(Arrays.asList(
                KafkaProduceResponse.success(),
                KafkaProduceResponse.failure(new TimeoutException("timeout"))));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertTrue(response.hasErrors());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(1L).getErrorType());
        Assert.assertNull(response.getErrorsFor(0L));
    }

    @Test
    public void shouldReportAllRecordsAsFailedWhenClientThrows() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenThrow(new KafkaException("producer closed"));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, response.getErrorsFor(1L).getErrorType());
    }

    @Test
    public void shouldCaptureFailureMetricsWhenWholeBatchFailsToProduce() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenThrow(new KafkaException("producer closed"));
        when(metrics.getKafkaFailureResponseTotalMetric()).thenReturn("failure_total");
        when(metrics.getKafkaErrorsTotalMetric()).thenReturn("errors_total");
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        verify(instrumentation, Mockito.times(2)).incrementCounter("failure_total");
        verify(instrumentation, Mockito.times(2)).incrementCounter("errors_total", "error_type=SINK_NON_RETRYABLE_ERROR");
    }

    @Test
    public void shouldCloseClient() throws IOException {
        kafkaSink.close();
        verify(kafkaSinkClient, Mockito.times(1)).close();
    }

    @Test
    public void shouldReturnEmptyResponseForEmptyBatch() {
        List<Message> messages = new ArrayList<>();
        when(recordParser.convert(messages)).thenReturn(new ArrayList<>());
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertFalse(response.hasErrors());
        verify(kafkaSinkClient, never()).send(Mockito.anyList());
    }

    @Test
    public void shouldCombineParsingAndProduceErrorsInResponse() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.invalidRecord(0L, new ErrorInfo(new IOException("parse error"), ErrorType.DESERIALIZATION_ERROR), "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(2L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        List<KafkaRecord> validRecords = records.stream().filter(KafkaRecord::isValid).collect(Collectors.toList());
        when(kafkaSinkClient.send(validRecords)).thenReturn(Arrays.asList(
                KafkaProduceResponse.success(),
                KafkaProduceResponse.failure(new TimeoutException("timeout"))));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, response.getErrorsFor(0L).getErrorType());
        Assert.assertNull(response.getErrorsFor(1L));
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(2L).getErrorType());
    }

    @Test
    public void shouldReportAllRecordsAsRetryableWhenClientThrowsRetriableException() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenThrow(new TimeoutException("flush timed out"));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(2, response.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(0L).getErrorType());
        Assert.assertEquals(ErrorType.SINK_RETRYABLE_ERROR, response.getErrorsFor(1L).getErrorType());
    }

    @Test
    public void shouldReportAllRecordsAsUnknownWhenClientThrowsUnexpectedException() {
        List<Message> messages = new ArrayList<>();
        List<KafkaRecord> records = Arrays.asList(KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"));
        when(recordParser.convert(messages)).thenReturn(records);
        when(kafkaSinkClient.send(records)).thenThrow(new IllegalStateException("unexpected state"));
        SinkResponse response = kafkaSink.pushToSink(messages);
        Assert.assertEquals(ErrorType.SINK_UNKNOWN_ERROR, response.getErrorsFor(0L).getErrorType());
    }
}
