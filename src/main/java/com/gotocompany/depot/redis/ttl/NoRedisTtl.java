package com.gotocompany.depot.redis.ttl;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * {@link RedisTtl} that applies no expiry, leaving written keys to persist indefinitely.
 *
 * <p>Selected when {@code SINK_REDIS_TTL_TYPE} is
 * {@link com.gotocompany.depot.redis.enums.RedisSinkTtlType#DISABLE}. Both overloads return
 * {@code null}, which the response wrappers report as a {@code NoOp} TTL status.</p>
 */
public class NoRedisTtl implements RedisTtl {
    /**
     * Applies no TTL on the pipeline.
     *
     * @param jedisPipelined the pipeline (ignored)
     * @param key the key (ignored)
     * @return always {@code null}, indicating no expiry command was queued
     */
    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return null;
    }

    /**
     * Applies no TTL on the cluster connection.
     *
     * @param jedisCluster the cluster connection (ignored)
     * @param key the key (ignored)
     * @return always {@code null}, indicating no expiry command was issued
     */
    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return null;
    }
}
