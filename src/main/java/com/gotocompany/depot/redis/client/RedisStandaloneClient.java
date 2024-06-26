package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.RedisSinkMetrics;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTTLFactory;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import com.gotocompany.depot.redis.util.RedisSinkUtils;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis standalone client.
 */
@AllArgsConstructor
public class RedisStandaloneClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private final DefaultJedisClientConfig defaultJedisClientConfig;
    private final HostAndPort hostAndPort;
    private Jedis jedis;
    private final int connectionMaxRetries;
    private final long connectionRetryBackoffMs;
    private final RedisSinkMetrics redisSinkMetrics;

    public RedisStandaloneClient(Instrumentation instrumentation, RedisSinkConfig config) {
        this.instrumentation = instrumentation;
        this.connectionMaxRetries = config.getSinkRedisConnectionMaxRetries();
        this.connectionRetryBackoffMs = config.getSinkRedisConnectionRetryBackoffMs();
        this.redisTTL = RedisTTLFactory.getTTl(config);
        this.defaultJedisClientConfig = RedisSinkUtils.getJedisConfig(config);
        this.hostAndPort = getHostPort(config);
        this.redisSinkMetrics = new RedisSinkMetrics(config);
    }

    private static HostAndPort getHostPort(RedisSinkConfig config) {
        try {
            return HostAndPort.parseString(StringUtils.trim(config.getSinkRedisUrls()));
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url for redis standalone: %s", config.getSinkRedisUrls()));
        }
    }

    /**
     * Pushes records in a transaction.
     * if the transaction fails, whole batch can be retried.
     *
     * @param records records to send
     * @return Custom response containing status of the API calls.
     */


    @Override
    public List<RedisResponse> send(List<RedisRecord> records) {
        int retryCount = connectionMaxRetries;
        List<RedisResponse> redisResponseList = null;
        while (retryCount >= 0) {
            try {
                redisResponseList = sendInternal(records);
                instrumentSuccess(redisResponseList);
                break;
            } catch (RuntimeException e) {

                e.printStackTrace();
                if (retryCount == 0) {
                    instrumentFailure(records);
                    throw e;
                }
                instrumentation.logInfo("Backing off for " + connectionRetryBackoffMs + " milliseconds.");
                try {
                    Thread.sleep(connectionRetryBackoffMs);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
                instrumentation.logInfo("Attempting to recreate Redis client. Retry attempt count : " + (connectionMaxRetries - retryCount + 1));
                instrumentConnectionRetry();
                this.init();

            }
            retryCount--;
        }
        return redisResponseList;
    }

    public List<RedisResponse> sendInternal(List<RedisRecord> records) {
        Pipeline jedisPipelined = jedis.pipelined();
        jedisPipelined.multi();
        List<RedisStandaloneResponse> responses = records.stream()
                .map(redisRecord -> redisRecord.send(jedisPipelined, redisTTL))
                .collect(Collectors.toList());
        Response<List<Object>> executeResponse = jedisPipelined.exec();
        jedisPipelined.sync();
        instrumentation.logDebug("jedis responses: {}", executeResponse.get());
        return responses.stream().map(RedisStandaloneResponse::process).collect(Collectors.toList());
    }


    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }

    public void init() {
        instrumentation.logInfo("Initialising Jedis client: Host: {} Port: {}", hostAndPort.getHost(), hostAndPort.getPort());
        jedis = new Jedis(hostAndPort, defaultJedisClientConfig);
    }

    private void instrumentSuccess(List<RedisResponse> redisResponseList) {
        long successCount = redisResponseList.size();
        instrumentation.captureCount(redisSinkMetrics.getRedisSuccessResponseTotalMetric(), successCount);
    }

    private void instrumentFailure(List<RedisRecord> validRecords) {
        long failureCount = validRecords.size();
        instrumentation.captureCount(redisSinkMetrics.getRedisNoResponseTotalMetric(), failureCount);
    }

    private void instrumentConnectionRetry() {
        instrumentation.captureCount(redisSinkMetrics.getRedisConnectionRetryTotalMetric(), 1L);
    }
}
