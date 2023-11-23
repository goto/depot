package com.gotocompany.depot.redis.client;

import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.client.response.RedisResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.record.RedisRecord;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Redis standalone client.
 */
@AllArgsConstructor
public class RedisStandaloneClient implements RedisClient {

    private final Instrumentation instrumentation;
    private final RedisTtl redisTTL;
    private Jedis jedis;
    private final DefaultJedisClientConfig defaultJedisClientConfig;
    private final HostAndPort hostAndPort;

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
        try {
            Response<List<Object>> executeResponse = jedisPipelined.exec();
            jedisPipelined.sync();
            instrumentation.logDebug("jedis responses: {}", executeResponse.get());
        } catch (ClassCastException | JedisConnectionException e) {
            recreate();
            throw e;
        }
        return responses.stream().map(RedisStandaloneResponse::process).collect(Collectors.toList());
    }

    @Override
    public void close() {
        instrumentation.logInfo("Closing Jedis client");
        jedis.close();
    }

    public void recreate() {

        jedis = new Jedis(hostAndPort, defaultJedisClientConfig);
    }
}
