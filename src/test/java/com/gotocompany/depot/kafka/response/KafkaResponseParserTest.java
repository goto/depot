package com.gotocompany.depot.kafka.response;

import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.kafka.record.KafkaRecord;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.KafkaSinkMetrics;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KafkaResponseParserTest {

    @Mock
    private Instrumentation instrumentation;
    private KafkaSinkMetrics metrics;

    @Before
    public void setup() {
        com.gotocompany.depot.config.SinkConfig config = Mockito.mock(com.gotocompany.depot.config.SinkConfig.class);
        when(config.getMetricsApplicationPrefix()).thenReturn("application_");
        metrics = new KafkaSinkMetrics(config);
    }

    @Test
    public void shouldReturnEmptyErrorsForSuccessfulResponses() {
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        List<KafkaProduceResponse> responses = Arrays.asList(
                KafkaProduceResponse.success(),
                KafkaProduceResponse.success());
        Map<Long, ErrorInfo> errors = KafkaResponseParser.getErrors(records, responses, metrics, instrumentation);
        assertTrue(errors.isEmpty());
        Mockito.verify(instrumentation, Mockito.times(2))
                .incrementCounter("application_sink_kafka_success_response_total");
    }

    @Test
    public void shouldMapFailedResponsesToErrors() {
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(3L, null, new byte[]{2}, "{}"),
                KafkaRecord.validRecord(5L, null, new byte[]{3}, "{}"));
        List<KafkaProduceResponse> responses = Arrays.asList(
                KafkaProduceResponse.success(),
                KafkaProduceResponse.failure(new TimeoutException("timeout")),
                KafkaProduceResponse.failure(new RecordTooLargeException("too large")));
        Map<Long, ErrorInfo> errors = KafkaResponseParser.getErrors(records, responses, metrics, instrumentation);
        assertEquals(2, errors.size());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errors.get(3L).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, errors.get(5L).getErrorType());
        Mockito.verify(instrumentation, Mockito.times(1))
                .incrementCounter("application_sink_kafka_success_response_total");
        Mockito.verify(instrumentation, Mockito.times(2))
                .incrementCounter("application_sink_kafka_failure_response_total");
    }

    @Test
    public void shouldCaptureErrorsTotalTaggedByErrorType() {
        List<KafkaRecord> records = Arrays.asList(
                KafkaRecord.validRecord(0L, null, new byte[]{1}, "{}"),
                KafkaRecord.validRecord(1L, null, new byte[]{2}, "{}"));
        List<KafkaProduceResponse> responses = Arrays.asList(
                KafkaProduceResponse.failure(new TimeoutException("timeout")),
                KafkaProduceResponse.failure(new RecordTooLargeException("too large")));
        KafkaResponseParser.getErrors(records, responses, metrics, instrumentation);
        Mockito.verify(instrumentation, Mockito.times(1))
                .incrementCounter("application_sink_kafka_errors_total", "error_type=SINK_RETRYABLE_ERROR");
        Mockito.verify(instrumentation, Mockito.times(1))
                .incrementCounter("application_sink_kafka_errors_total", "error_type=SINK_NON_RETRYABLE_ERROR");
    }

    @Test
    public void shouldMapRetriableExceptionToRetryableError() {
        ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(new TimeoutException("timeout"));
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR, errorInfo.getErrorType());
        assertEquals("timeout", errorInfo.getException().getMessage());
    }

    @Test
    public void shouldMapKafkaExceptionToNonRetryableError() {
        ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(new RecordTooLargeException("too large"));
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, errorInfo.getErrorType());
    }

    @Test
    public void shouldMapUnknownExceptionToUnknownError() {
        ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(new RuntimeException("unexpected"));
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, errorInfo.getErrorType());
    }

    @Test
    public void shouldWrapNonExceptionThrowable() {
        ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(new AssertionError("fatal"));
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, errorInfo.getErrorType());
        assertTrue(errorInfo.getException() instanceof RuntimeException);
    }

    @Test
    public void shouldClassifyCommonRetriableKafkaErrors() {
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.NetworkException("network error")).getErrorType());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("unknown topic")).getErrorType());
        assertEquals(ErrorType.SINK_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.NotEnoughReplicasException("not enough replicas")).getErrorType());
    }

    @Test
    public void shouldClassifyCommonNonRetriableKafkaErrors() {
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.SerializationException("bad bytes")).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.AuthenticationException("auth failed")).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.TopicAuthorizationException("not authorized")).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.errors.InvalidTopicException("bad topic name")).getErrorType());
        assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR,
                KafkaResponseParser.getErrorInfo(new org.apache.kafka.common.KafkaException("generic kafka error")).getErrorType());
    }

    @Test
    public void shouldClassifyInterruptedExceptionAsUnknownError() {
        ErrorInfo errorInfo = KafkaResponseParser.getErrorInfo(new InterruptedException("interrupted"));
        assertEquals(ErrorType.SINK_UNKNOWN_ERROR, errorInfo.getErrorType());
    }

    @Test
    public void shouldReturnEmptyErrorsForEmptyRecords() {
        Map<Long, ErrorInfo> errors = KafkaResponseParser.getErrors(
                java.util.Collections.emptyList(), java.util.Collections.emptyList(), metrics, instrumentation);
        assertTrue(errors.isEmpty());
        Mockito.verifyNoInteractions(instrumentation);
    }
}
