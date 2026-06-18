package com.gotocompany.depot.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * Interface for RedisTTL.
 *
 * <p>An instance is resolved once from configuration by {@link RedisTTLFactory} and shared by the
 * client. The two overloads mirror the two execution paths: one queues an expiry command on a
 * standalone pipeline, the other applies it directly on a cluster connection. Implementations include
 * {@link ExactTimeTtl} ({@code EXPIREAT}), {@link DurationTtl} ({@code EXPIRE}) and {@link NoRedisTtl}
 * (no expiry).</p>
 *
 * @see RedisTTLFactory
 */
public interface RedisTtl {
    /**
     * Queues a TTL command for the given key on a standalone pipeline.
     *
     * @param jedisPipelined the pipeline the expiry command is queued on
     * @param key the Redis key to apply the TTL to
     * @return a deferred {@link Response} holding the result of the expiry command (typically
     *     {@code 1} when applied and {@code 0} when not), or {@code null} when no TTL is applied
     */
    Response<Long> setTtl(Pipeline jedisPipelined, String key);

    /**
     * Applies a TTL command for the given key on a cluster connection.
     *
     * @param jedisCluster the cluster connection the expiry command is executed on
     * @param key the Redis key to apply the TTL to
     * @return the result of the expiry command (typically {@code 1} when applied and {@code 0} when
     *     not), or {@code null} when no TTL is applied
     */
    Long setTtl(JedisCluster jedisCluster, String key);
}
