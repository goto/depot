package com.gotocompany.depot.redis.client.entry;

import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Class for Redis Hash set entry.
 *
 * <p>Holds the resolved list key and the value to push, together with the {@link Instrumentation}
 * used for debug logging. It is produced by
 * {@link com.gotocompany.depot.redis.parsers.RedisListEntryParser} for sinks whose data type is
 * {@code LIST}. The {@link Instrumentation} field is excluded from {@code equals}/{@code hashCode} so
 * entries compare purely on key and value.</p>
 *
 * @see RedisKeyValueEntry
 * @see RedisHashSetFieldEntry
 */
@AllArgsConstructor
@EqualsAndHashCode
public class RedisListEntry implements RedisEntry {
    /**
     * Redis list key the value is appended to.
     */
    private final String key;
    /**
     * Value pushed onto the list.
     */
    private final String value;
    /**
     * Used for debug logging; excluded from equality so entries compare on key and value only.
     */
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    /**
     * Queues an {@code LPUSH} of the value onto the list key, plus any TTL command, on the pipeline.
     *
     * @param jedisPipelined the pipeline the {@code LPUSH} is queued on
     * @param redisTTL the TTL strategy applied to the list key
     * @return a deferred {@link RedisStandaloneResponse} labelled {@code LPUSH}
     */
    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        Response<Long> response = jedisPipelined.lpush(key, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("LPUSH", response, ttlResponse);
    }

    /**
     * Executes an {@code LPUSH} of the value onto the list key, plus any TTL command, on the cluster.
     *
     * @param jedisCluster the cluster connection the {@code LPUSH} is executed on
     * @param redisTTL the TTL strategy applied to the list key
     * @return a {@link RedisClusterResponse} labelled {@code LPUSH} on success, or a failed response
     *     carrying the {@code JedisException} message
     */
    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        try {
            Long response = jedisCluster.lpush(key, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("LPUSH", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    /**
     * Returns a debug representation of this entry including its key and value.
     *
     * @return a human-readable string containing the list key and value
     */
    @Override
    public String toString() {
        return String.format("RedisListEntry: Key %s, Value %s", key, value);
    }
}
