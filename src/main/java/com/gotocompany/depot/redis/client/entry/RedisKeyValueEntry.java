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
 * {@link RedisEntry} that writes a single key/value pair via {@code SET}.
 *
 * <p>Holds the resolved key and value, together with the {@link Instrumentation} used for debug
 * logging. It is produced by {@link com.gotocompany.depot.redis.parsers.RedisKeyValueEntryParser} for
 * sinks whose data type is {@code KEYVALUE}. The {@link Instrumentation} field is excluded from
 * {@code equals}/{@code hashCode} so entries compare purely on key and value.</p>
 *
 * @see RedisListEntry
 * @see RedisHashSetFieldEntry
 */
@AllArgsConstructor
@EqualsAndHashCode
public class RedisKeyValueEntry implements RedisEntry {
    /**
     * Redis key to set.
     */
    private final String key;
    /**
     * Value stored at the key.
     */
    private final String value;
    /**
     * Used for debug logging; excluded from equality so entries compare on key and value only.
     */
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    /**
     * Queues a {@code SET} of the key to the value, plus any TTL command, on the pipeline.
     *
     * @param jedisPipelined the pipeline the {@code SET} is queued on
     * @param redisTTL the TTL strategy applied to the key
     * @return a deferred {@link RedisStandaloneResponse} labelled {@code SET}
     */
    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        Response<String> response = jedisPipelined.set(key, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("SET", response, ttlResponse);
    }

    /**
     * Executes a {@code SET} of the key to the value, plus any TTL command, on the cluster.
     *
     * @param jedisCluster the cluster connection the {@code SET} is executed on
     * @param redisTTL the TTL strategy applied to the key
     * @return a {@link RedisClusterResponse} labelled {@code SET} on success, or a failed response
     *     carrying the {@code JedisException} message
     */
    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, value: {}", key, value);
        try {
            String response = jedisCluster.set(key, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("SET", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    /**
     * Returns a debug representation of this entry including its key and value.
     *
     * @return a human-readable string containing the key and value
     */
    @Override
    public String toString() {
        return String.format("RedisKeyValueEntry: Key %s, Value %s", key, value);
    }
}
