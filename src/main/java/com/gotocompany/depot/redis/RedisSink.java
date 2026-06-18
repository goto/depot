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

/**
 * {@link Sink} implementation that writes batches of messages into Redis.
 *
 * <p>A {@code RedisSink} is the terminal stage of the Redis sink pipeline. For each batch handed to
 * {@link #pushToSink(List)} it delegates conversion of the raw {@link Message} objects into
 * {@link RedisRecord} instances to a {@link RedisParser}, separates records that failed to parse from
 * those that are valid, attempts to write the valid records to Redis through a {@link RedisClient},
 * and finally aggregates every parse-time and write-time failure into a single {@link SinkResponse}
 * keyed by the original message index.</p>
 *
 * <p>Instances are created per worker by {@link RedisSinkFactory#create()}; because the underlying
 * Jedis client is not thread safe, a {@code RedisSink} (together with the {@link RedisClient} it owns)
 * is intended to be used by a single thread at a time.</p>
 *
 * @see RedisSinkFactory
 * @see RedisClient
 * @see RedisParser
 */
public class RedisSink implements Sink {
    /**
     * Client used to write the valid records of each batch to the configured Redis deployment.
     */
    private final RedisClient redisClient;
    /**
     * Parser that converts incoming messages into {@link RedisRecord} instances, flagging each as
     * valid or invalid.
     */
    private final RedisParser redisParser;
    /**
     * Logging and metrics facade used to report batch progress.
     */
    private final Instrumentation instrumentation;


    /**
     * Creates a Redis sink backed by the supplied client, parser and instrumentation.
     *
     * @param redisClient the client used to push records to Redis
     * @param redisParser the parser that converts messages into {@link RedisRecord} instances
     * @param instrumentation the instrumentation used for logging and metrics
     */
    public RedisSink(RedisClient redisClient, RedisParser redisParser, Instrumentation instrumentation) {
        this.redisClient = redisClient;
        this.redisParser = redisParser;
        this.instrumentation = instrumentation;

    }

    /**
     * Converts a batch of messages into Redis records and writes the valid ones to Redis.
     *
     * <p>The supplied messages are first converted with the configured {@link RedisParser}. The
     * resulting records are partitioned by {@code RedisRecord#isValid()}: invalid records (those that
     * failed to parse) contribute their {@link ErrorInfo} to the response immediately, while valid
     * records are forwarded to {@link #send(List)}. Any errors returned for the valid records are
     * merged into the same response, and a successful write of a non-empty batch is logged.</p>
     *
     * @param messages the batch of messages to push to Redis
     * @return a {@link SinkResponse} whose errors map associates the index of every failed message
     *     with its {@link ErrorInfo}; the map is empty when the whole batch succeeds
     */
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

    /**
     * Writes the already-validated records to Redis and maps any failures back to message indexes.
     *
     * <p>The records are handed to {@link RedisClient#send(List)}. If that call throws a
     * {@link RuntimeException} (for example because the connection could not be re-established after
     * all retries) every record in the batch is reported as a non-retryable error via
     * {@link RedisSinkUtils#getNonRetryableErrors(List, RuntimeException, Instrumentation)}. Otherwise
     * the per-record responses are inspected and only the failed ones are translated into errors by
     * {@link RedisSinkUtils#getErrorsFromResponse(List, List, Instrumentation)}.</p>
     *
     * @param validRecords the records that parsed successfully and are ready to be written
     * @return a map from message index to {@link ErrorInfo} for every record that could not be
     *     written; empty when all records were written successfully
     */
    private Map<Long, ErrorInfo> send(List<RedisRecord> validRecords) {
        List<RedisResponse> responses;
        try {
            responses = redisClient.send(validRecords);
        } catch (RuntimeException e) {
            return RedisSinkUtils.getNonRetryableErrors(validRecords, e, instrumentation);
        }
        return RedisSinkUtils.getErrorsFromResponse(validRecords, responses, instrumentation);

    }

    /**
     * Releases resources held by this sink.
     *
     * <p>This implementation is intentionally a no-op: the {@link RedisClient} owned by the sink
     * manages its own Jedis connection lifecycle and is closed independently, so there is nothing to
     * release here.</p>
     *
     * @throws IOException declared by {@link Sink#close()}; never actually thrown by this
     *     implementation
     */
    @Override
    public void close() throws IOException {

    }
}
