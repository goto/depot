package com.gotocompany.depot.redis.ttl;

import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;


/**
 * {@link RedisTtl} that expires keys at an absolute point in time using {@code EXPIREAT}.
 *
 * <p>Selected when {@code SINK_REDIS_TTL_TYPE} is
 * {@link com.gotocompany.depot.redis.enums.RedisSinkTtlType#EXACT_TIME}. Every written key is set to
 * expire at the configured Unix timestamp.</p>
 */
@AllArgsConstructor
public class ExactTimeTtl implements RedisTtl {
    /**
     * Absolute expiry time, as a Unix timestamp in seconds, applied to every written key.
     */
    private long unixTime;

    /**
     * Queues an {@code EXPIREAT} for the key at the configured absolute time on the pipeline.
     *
     * @param jedisPipelined the pipeline the {@code EXPIREAT} is queued on
     * @param key the Redis key to expire
     * @return a deferred {@link Response} holding {@code 1} if the timeout was set and {@code 0}
     *     otherwise
     */
    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return jedisPipelined.expireAt(key, unixTime);
    }

    /**
     * Applies an {@code EXPIREAT} for the key at the configured absolute time on the cluster.
     *
     * @param jedisCluster the cluster connection the {@code EXPIREAT} is executed on
     * @param key the Redis key to expire
     * @return {@code 1} if the timeout was set and {@code 0} otherwise
     */
    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return jedisCluster.expireAt(key, unixTime);
    }
}
