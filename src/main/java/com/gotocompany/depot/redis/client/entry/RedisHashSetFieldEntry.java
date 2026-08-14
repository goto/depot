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
 * <p>Holds the resolved hash key, the field name within that hash and the field value, together with
 * the {@link Instrumentation} used for debug logging. A single message can yield several of these
 * entries (one per configured field) via
 * {@link com.gotocompany.depot.redis.parsers.RedisHashSetEntryParser} for sinks whose data type is
 * {@code HASHSET}. The {@link Instrumentation} field is excluded from {@code equals}/{@code hashCode}
 * so entries compare purely on key, field and value.</p>
 *
 * @see RedisKeyValueEntry
 * @see RedisListEntry
 */
@AllArgsConstructor
@EqualsAndHashCode
public class RedisHashSetFieldEntry implements RedisEntry {

    /**
     * Key of the Redis hash being written.
     */
    private final String key;
    /**
     * Field within the hash that is set.
     */
    private final String field;
    /**
     * Value assigned to the field.
     */
    private final String value;
    /**
     * Used for debug logging; excluded from equality so entries compare on key, field and value only.
     */
    @EqualsAndHashCode.Exclude
    private final Instrumentation instrumentation;

    /**
     * Returns the hash key of this entry.
     *
     * @return the Redis hash key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the hash field name of this entry.
     *
     * @return the field within the hash
     */
    public String getField() {
        return field;
    }

    /**
     * Returns the value of this entry.
     *
     * @return the value assigned to the field
     */
    public String getValue() {
        return value;
    }

    /**
     * Queues an {@code HSET} of the field to the value, plus any TTL command, on the pipeline.
     *
     * @param jedisPipelined the pipeline the {@code HSET} is queued on
     * @param redisTTL the TTL strategy applied to the hash key
     * @return a deferred {@link RedisStandaloneResponse} labelled {@code HSET}
     */
    @Override
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        Response<Long> response = jedisPipelined.hset(key, field, value);
        Response<Long> ttlResponse = redisTTL.setTtl(jedisPipelined, key);
        return new RedisStandaloneResponse("HSET", response, ttlResponse);
    }

    /**
     * Executes an {@code HSET} of the field to the value, plus any TTL command, on the cluster.
     *
     * @param jedisCluster the cluster connection the {@code HSET} is executed on
     * @param redisTTL the TTL strategy applied to the hash key
     * @return a {@link RedisClusterResponse} labelled {@code HSET} on success, or a failed response
     *     carrying the {@code JedisException} message
     */
    @Override
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        instrumentation.logDebug("key: {}, field: {}, value: {}", key, field, value);
        try {
            Long response = jedisCluster.hset(key, field, value);
            Long ttlResponse = redisTTL.setTtl(jedisCluster, key);
            return new RedisClusterResponse("HSET", response, ttlResponse);
        } catch (JedisException e) {
            return new RedisClusterResponse(e.getMessage());
        }
    }

    /**
     * Returns a debug representation of this entry including its key, field and value.
     *
     * @return a human-readable string containing the hash key, field and value
     */
    @Override
    public String toString() {
        return String.format("RedisHashSetFieldEntry Key %s, Field %s, Value %s", key, field, value);
    }
}
