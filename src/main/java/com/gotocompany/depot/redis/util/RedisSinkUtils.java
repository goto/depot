package com.gotocompany.depot.redis.util;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.metrics.Instrumentation;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Static helpers shared across the Redis sink for error mapping and client configuration.
 *
 * <p>It translates per-record responses and connection-level failures into the {@link ErrorInfo} map
 * keyed by message index that the sink reports, and builds the {@link DefaultJedisClientConfig} used
 * to open Jedis connections from the sink configuration.</p>
 */
public class RedisSinkUtils {
    /**
     * Maps the failed responses of a batch to per-message errors.
     *
     * <p>The {@code responses} list is iterated positionally against {@code redisRecords}; for every
     * response whose {@link RedisResponse#isFailed()} is {@code true} the matching record is logged
     * and an {@link ErrorInfo} of type {@link ErrorType#DEFAULT_ERROR} (wrapping the response message)
     * is stored under the record's index. Successful responses contribute nothing.</p>
     *
     * @param redisRecords the records that were sent, aligned by position with {@code responses}
     * @param responses the per-record responses returned by the client
     * @param instrumentation the instrumentation used to log each failure
     * @return a map from message index to {@link ErrorInfo} for each failed response; empty when all
     *     responses succeeded
     */
    public static Map<Long, ErrorInfo> getErrorsFromResponse(List<RedisRecord> redisRecords, List<RedisResponse> responses, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        IntStream.range(0, responses.size()).forEach(
                index -> {
                    RedisResponse response = responses.get(index);
                    if (response.isFailed()) {
                        RedisRecord record = redisRecords.get(index);
                        instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                                record.toString(), response.getMessage());
                        errors.put(record.getIndex(), new ErrorInfo(new Exception(response.getMessage()), ErrorType.DEFAULT_ERROR));
                    }
                }
        );
        return errors;
    }

    /**
     * Marks every record of a batch as failed with a non-retryable error.
     *
     * <p>Used when the client throws before producing per-record responses (for example a connection
     * failure). Each record is logged and associated with an {@link ErrorInfo} of type
     * {@link ErrorType#SINK_NON_RETRYABLE_ERROR} wrapping the exception's message.</p>
     *
     * @param redisRecords the records that could not be written
     * @param e the exception that caused the whole batch to fail
     * @param instrumentation the instrumentation used to log each failure
     * @return a map from message index to a non-retryable {@link ErrorInfo} for every record
     */
    public static Map<Long, ErrorInfo> getNonRetryableErrors(List<RedisRecord> redisRecords, RuntimeException e, Instrumentation instrumentation) {
        Map<Long, ErrorInfo> errors = new HashMap<>();
        for (RedisRecord record : redisRecords) {
            instrumentation.logError("Error while inserting to redis for message. Record: {}, Error: {}",
                    record.toString(), e.getMessage());
            errors.put(record.getIndex(), new ErrorInfo(new Exception(e.getMessage()), ErrorType.SINK_NON_RETRYABLE_ERROR));

        }
        return errors;
    }


    /**
     * Builds the Jedis client configuration from the sink configuration.
     *
     * <p>Applies the configured connection and socket timeouts and the authentication username and
     * password to a {@link DefaultJedisClientConfig}.</p>
     *
     * @param config the Redis sink configuration
     * @return a {@link DefaultJedisClientConfig} carrying the configured timeouts and credentials
     */
    public static DefaultJedisClientConfig getJedisConfig(RedisSinkConfig config) {
        return DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(config.getSinkRedisConnectionTimeoutMs())
                .socketTimeoutMillis(config.getSinkRedisSocketTimeoutMs())
                .user(config.getSinkRedisAuthUsername())
                .password(config.getSinkRedisAuthPassword())
                .build();
    }
}
