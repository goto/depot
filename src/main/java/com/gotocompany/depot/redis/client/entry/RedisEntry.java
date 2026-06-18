package com.gotocompany.depot.redis.client.entry;

import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

/**
 * The interface Redis data entry.
 *
 * <p>Each implementation knows how to issue the Redis command appropriate for one of the supported
 * data types ({@link RedisKeyValueEntry} for {@code SET}, {@link RedisListEntry} for {@code LPUSH}
 * and {@link RedisHashSetFieldEntry} for {@code HSET}) and how to apply the configured
 * {@link RedisTtl} to the affected key. The interface offers two {@code send} overloads so the same
 * entry can be written either through a standalone pipeline or through a cluster connection.</p>
 *
 * @see com.gotocompany.depot.redis.record.RedisRecord
 * @see RedisTtl
 */
public interface RedisEntry {

    /**
     * Push messages to jedis pipeline.
     *
     * <p>The command is added to the supplied {@link Pipeline} for later transactional execution, and
     * the given {@link RedisTtl} may add a companion expiry command for the same key. The returned
     * response wraps the deferred handles and must be resolved after the pipeline has been
     * executed.</p>
     *
     * @param jedisPipelined the jedis pipelined
     * @param redisTTL       the redis ttl
     * @return a deferred {@link RedisStandaloneResponse} for the queued command
     */
    RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL);

    /**
     * Push message to jedis cluster.
     *
     * <p>The command is run immediately on the supplied {@link JedisCluster}, and the given
     * {@link RedisTtl} may apply an expiry to the same key. Any {@code JedisException} raised by Redis
     * is captured and surfaced as a failed response rather than propagated.</p>
     *
     * @param jedisCluster the jedis cluster
     * @param redisTTL     the redis ttl
     * @return a {@link RedisClusterResponse} describing the outcome
     */
    RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL);
}
