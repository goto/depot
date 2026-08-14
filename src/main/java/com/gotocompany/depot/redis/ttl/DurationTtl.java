package com.gotocompany.depot.redis.ttl;

import lombok.AllArgsConstructor;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;


/**
 * {@link RedisTtl} that expires keys after a relative duration using {@code EXPIRE}.
 *
 * <p>Selected when {@code SINK_REDIS_TTL_TYPE} is
 * {@link com.gotocompany.depot.redis.enums.RedisSinkTtlType#DURATION}. Every written key is set to
 * expire the configured number of seconds after the command is applied.</p>
 */
@AllArgsConstructor
public class DurationTtl implements RedisTtl {
    /**
     * Time-to-live, in seconds, applied relative to when each key is written.
     */
    private int ttlInSeconds;

    /**
     * Queues an {@code EXPIRE} for the key with the configured duration on the pipeline.
     *
     * @param jedisPipelined the pipeline the {@code EXPIRE} is queued on
     * @param key the Redis key to expire
     * @return a deferred {@link Response} holding {@code 1} if the timeout was set and {@code 0}
     *     otherwise
     */
    @Override
    public Response<Long> setTtl(Pipeline jedisPipelined, String key) {
        return jedisPipelined.expire(key, ttlInSeconds);
    }

    /**
     * Applies an {@code EXPIRE} for the key with the configured duration on the cluster.
     *
     * @param jedisCluster the cluster connection the {@code EXPIRE} is executed on
     * @param key the Redis key to expire
     * @return {@code 1} if the timeout was set and {@code 0} otherwise
     */
    @Override
    public Long setTtl(JedisCluster jedisCluster, String key) {
        return jedisCluster.expire(key, ttlInSeconds);
    }
}
