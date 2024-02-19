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

@RunWith(MockitoJUnitRunner.class)
public class RedisSinkTest {
    @Mock
    private RedisClient redisClient;
    @Mock
    private RedisParser redisParser;
    @Mock
    private Instrumentation instrumentation;

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
