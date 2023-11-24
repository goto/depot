package com.gotocompany.depot.redis;

import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.client.RedisClient;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.parsers.RedisParser;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.util.RedisSinkUtils;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RedisSink implements Sink {
    private final RedisClient redisClient;
    private final RedisParser redisParser;
    private final Instrumentation instrumentation;
    private static final int CONNECTION_RETRY = 2;

    public RedisSink(RedisClient redisClient, RedisParser redisParser, Instrumentation instrumentation) {
        this.redisClient = redisClient;
        this.redisParser = redisParser;
        this.instrumentation = instrumentation;
    }

    @Override
    public SinkResponse pushToSink(List<Message> messages) {
        List<RedisRecord> records = redisParser.convert(messages);
        Map<Boolean, List<RedisRecord>> splitterRecords = records.stream().collect(Collectors.partitioningBy(RedisRecord::isValid));
        List<RedisRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<RedisRecord> validRecords = splitterRecords.get(Boolean.TRUE);
        SinkResponse sinkResponse = new SinkResponse();
        invalidRecords.forEach(invalidRecord -> sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (!validRecords.isEmpty()) {
            Map<Long, ErrorInfo> errorInfoMap = send(validRecords);
            errorInfoMap.forEach(sinkResponse::addErrors);
            instrumentation.logInfo("Pushed a batch of {} records to Redis", validRecords.size());
        }
        return sinkResponse;
    }

    private Map<Long, ErrorInfo> send(List<RedisRecord> validRecords) {
        List<RedisResponse> responses = null;
        RuntimeException exception = null;
        int retry = CONNECTION_RETRY;
        while (retry > 0) {
            try {
                responses = redisClient.send(validRecords);
                break;
            } catch (RuntimeException e) {
                exception = e;
                e.printStackTrace();
                redisClient.init();
            }
            retry--;
        }
        if (responses == null) {
            return RedisSinkUtils.getNonRetryableErrors(validRecords, exception, instrumentation);
        } else {
            return RedisSinkUtils.getErrorsFromResponse(validRecords, responses, instrumentation);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
