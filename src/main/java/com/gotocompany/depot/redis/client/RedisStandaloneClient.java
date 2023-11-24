package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
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
public class RedisStandaloneClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private Jedis jedis;
    private final RedisSinkConfig redisSinkConfig;

    public RedisStandaloneClient(Instrumentation instrumentation, RedisTtl redisTTL, RedisSinkConfig redisSinkConfig) {
        this.instrumentation = instrumentation;
        this.redisTTL = redisTTL;
        this.redisSinkConfig = redisSinkConfig;
        init();
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
        HostAndPort hostAndPort;
        try {
            hostAndPort = HostAndPort.parseString(StringUtils.trim(redisSinkConfig.getSinkRedisUrls()));
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException(String.format("Invalid url for redis standalone: %s", redisSinkConfig.getSinkRedisUrls()));
        }
        DefaultJedisClientConfig jedisConfig = DefaultJedisClientConfig.builder()
                .user(redisSinkConfig.getSinkRedisAuthUsername())
                .password(redisSinkConfig.getSinkRedisAuthPassword())
                .build();
        jedis = new Jedis(hostAndPort, jedisConfig);
    }
}
