package com.gotocompany.depot.redis;

import com.gotocompany.depot.redis.client.RedisClient;
import com.gotocompany.depot.redis.client.entry.RedisListEntry;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.parsers.RedisParser;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RedisSink}, which converts messages with a {@link RedisParser}, writes the
 * valid records with a {@link RedisClient} and aggregates parse and write failures into a
 * {@link SinkResponse}.
 *
 * <p>The tests run under {@link MockitoJUnitRunner} with a mocked {@link RedisClient},
 * {@link RedisParser} and {@link Instrumentation}. They cover the all-success path, the reporting of
 * parse-time errors, per-record client failures, the combination of both, and the conversion of an
 * unexpected client exception into non-retryable errors for every record.</p>
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {
    /**
     * Mocked client whose per-record responses (or thrown errors) are stubbed.
     */
    @Mock
    private RedisClient redisClient;
    /**
     * Mocked parser stubbed to convert the input messages into records.
     */
    @Mock
    private RedisParser redisParser;
    /**
     * Mocked instrumentation supplied to the sink.
     */
    @Mock
    private Instrumentation instrumentation;

    /**
     * Verifies that a fully successful batch reports no errors.
     *
     * <p>Given the parser returning five valid records and the client returning five non-failed
     * responses, when {@link RedisSink#pushToSink} is called, then the resulting {@link SinkResponse}
     * has no errors.</p>
     */
    @Test
    public void shouldPushToSink() {
        List<Message> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 0L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 2L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        when(redisParser.convert(messages)).thenReturn(records);
        when(redisClient.send(records)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        SinkResponse sinkResponse = redisSink.pushToSink(messages);
        Assert.assertFalse(sinkResponse.hasErrors());
    }

    /**
     * Verifies that parse-time failures are surfaced while valid records are still written.
     *
     * <p>Given five records of which those at indices {@code 0} and {@code 2} are invalid (carrying a
     * {@link ErrorType#DESERIALIZATION_ERROR} and a {@link ErrorType#DEFAULT_ERROR} respectively) and
     * the remaining valid records are written successfully, when {@link RedisSink#pushToSink} is
     * called, then the {@link SinkResponse} has exactly two errors, mapped to indices {@code 0} and
     * {@code 2} with the matching error types.</p>
     */
    @Test
    public void shouldReportParsingErrors() {
        List<Message> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(null, 0L, new ErrorInfo(new IOException(""), ErrorType.DESERIALIZATION_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(null, 2L, new ErrorInfo(new ConfigurationException(""), ErrorType.DEFAULT_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        SinkResponse sinkResponse = redisSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(2, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
    }

    /**
     * Verifies that per-record client write failures are surfaced.
     *
     * <p>Given five valid records whose responses at indices {@code 2}, {@code 3} and {@code 4} are
     * failed with messages {@code "failed at 2"}, {@code "failed at 3"} and {@code "failed at 4"}, when
     * {@link RedisSink#pushToSink} is called, then the {@link SinkResponse} has three errors of type
     * {@link ErrorType#DEFAULT_ERROR}, each carrying the corresponding failure message.</p>
     */
    @Test
    public void shouldReportClientErrors() {
        List<Message> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 0L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 2L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        when(responses.get(2).isFailed()).thenReturn(true);
        when(responses.get(2).getMessage()).thenReturn("failed at 2");
        when(responses.get(3).isFailed()).thenReturn(true);
        when(responses.get(3).getMessage()).thenReturn("failed at 3");
        when(responses.get(4).isFailed()).thenReturn(true);
        when(responses.get(4).getMessage()).thenReturn("failed at 4");
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        when(redisClient.send(records)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        SinkResponse sinkResponse = redisSink.pushToSink(messages);
        Assert.assertTrue(sinkResponse.hasErrors());
        Assert.assertEquals(3, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, sinkResponse.getErrorsFor(3).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, sinkResponse.getErrorsFor(4).getErrorType());
        Assert.assertEquals("failed at 2", sinkResponse.getErrorsFor(2).getException().getMessage());
        Assert.assertEquals("failed at 3", sinkResponse.getErrorsFor(3).getException().getMessage());
        Assert.assertEquals("failed at 4", sinkResponse.getErrorsFor(4).getException().getMessage());
    }

    /**
     * Verifies that parse-time and client write failures are combined in one response.
     *
     * <p>Given records at indices {@code 0} and {@code 2} that failed to parse plus valid records at
     * indices {@code 1}, {@code 3} and {@code 4} whose written responses fail for indices {@code 3} and
     * {@code 4}, when {@link RedisSink#pushToSink} is called, then the {@link SinkResponse} carries four
     * errors: the two parse errors ({@link ErrorType#DESERIALIZATION_ERROR} at {@code 0} and
     * {@link ErrorType#DEFAULT_ERROR} at {@code 2}) and the two client failures with messages
     * {@code "failed at 3"} and {@code "failed at 4"}.</p>
     */
    @Test
    public void shouldReportNetErrors() {
        List<Message> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(null, 0L, new ErrorInfo(new IOException(""), ErrorType.DESERIALIZATION_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(null, 2L, new ErrorInfo(new ConfigurationException(""), ErrorType.DEFAULT_ERROR), null, false));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        List<RedisResponse> responses = new ArrayList<>();
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        responses.add(Mockito.mock(RedisResponse.class));
        when(responses.get(1).isFailed()).thenReturn(true);
        when(responses.get(1).getMessage()).thenReturn("failed at 3");
        when(responses.get(2).isFailed()).thenReturn(true);
        when(responses.get(2).getMessage()).thenReturn("failed at 4");
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenReturn(responses);
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        SinkResponse sinkResponse = redisSink.pushToSink(messages);
        Assert.assertEquals(4, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.DESERIALIZATION_ERROR, sinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.DEFAULT_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
        Assert.assertEquals("failed at 3", sinkResponse.getErrorsFor(3).getException().getMessage());
        Assert.assertEquals("failed at 4", sinkResponse.getErrorsFor(4).getException().getMessage());
    }

    /**
     * Verifies that an unexpected client exception fails the whole batch as non-retryable.
     *
     * <p>Given five valid records and a client that throws a {@link ClassCastException} when writing
     * them, when {@link RedisSink#pushToSink} is called, then the {@link SinkResponse} contains five
     * errors, all of type {@link ErrorType#SINK_NON_RETRYABLE_ERROR} and each carrying the exception
     * message.</p>
     */
    @Test
    public void shouldReturnNonRetryableErrors() {
        List<Message> messages = new ArrayList<>();
        List<RedisRecord> records = new ArrayList<>();
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 0L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 1L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 2L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 3L, null, null, true));
        records.add(new RedisRecord(new RedisListEntry("key1", "val1", null), 4L, null, null, true));
        when(redisParser.convert(messages)).thenReturn(records);
        List<RedisRecord> validRecords = records.stream().filter(RedisRecord::isValid).collect(Collectors.toList());
        when(redisClient.send(validRecords)).thenThrow(new ClassCastException("[B cannot be cast to java.util.List"));
        RedisSink redisSink = new RedisSink(redisClient, redisParser, instrumentation);
        SinkResponse sinkResponse = redisSink.pushToSink(messages);
        Assert.assertEquals(5, sinkResponse.getErrors().size());
        Assert.assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, sinkResponse.getErrorsFor(0).getErrorType());
        Assert.assertEquals(ErrorType.SINK_NON_RETRYABLE_ERROR, sinkResponse.getErrorsFor(2).getErrorType());
        Assert.assertEquals("[B cannot be cast to java.util.List", sinkResponse.getErrorsFor(3).getException().getMessage());
        Assert.assertEquals("[B cannot be cast to java.util.List", sinkResponse.getErrorsFor(4).getException().getMessage());
    }
}
