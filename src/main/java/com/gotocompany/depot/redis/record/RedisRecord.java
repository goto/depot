package com.gotocompany.depot.redis.record;

import com.gotocompany.depot.redis.client.entry.RedisEntry;
import com.gotocompany.depot.redis.client.response.RedisClusterResponse;
import com.gotocompany.depot.redis.client.response.RedisStandaloneResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.redis.ttl.RedisTtl;
import lombok.AllArgsConstructor;
import lombok.Getter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;


/**
 * One message after conversion for the Redis sink, pairing a writable entry with its bookkeeping.
 *
 * <p>A record is the unit the {@link com.gotocompany.depot.redis.RedisSink} works with. Successfully
 * parsed messages carry a non-null {@link RedisEntry} and are marked {@code valid}, while
 * messages that failed to parse carry a {@code null} entry, a populated {@link ErrorInfo} and are
 * marked invalid. The {@code index} ties the record back to the position of its source
 * message in the batch so that errors can be reported against the right message, and the
 * {@code metadata} provides a human-readable description used in logs.</p>
 *
 * <p>The two {@code send} overloads simply delegate to the wrapped {@link RedisEntry} and are intended
 * to be called only for valid records.</p>
 *
 * @see RedisEntry
 * @see com.gotocompany.depot.redis.util.RedisSinkUtils
 */
@AllArgsConstructor
public class RedisRecord {
    /**
     * Writable Redis command for a valid record, or {@code null} when the record is invalid.
     */
    private RedisEntry redisEntry;
    /**
     * Zero-based position of the source message within its batch.
     */
    @Getter
    private final Long index;
    /**
     * Parse-failure detail for an invalid record, or {@code null} when the record is valid.
     */
    @Getter
    private final ErrorInfo errorInfo;
    /**
     * Human-readable metadata of the source message, used in log messages.
     */
    @Getter
    private final String metadata;
    /**
     * Whether the record parsed successfully and can be written to Redis.
     */
    @Getter
    private final boolean valid;

    /**
     * Delegates to the wrapped entry to queue this record's command on a standalone pipeline.
     *
     * @param jedisPipelined the pipeline the command is queued on
     * @param redisTTL the TTL strategy applied to the entry's key
     * @return the deferred {@link RedisStandaloneResponse} produced by the wrapped {@link RedisEntry}
     * @throws NullPointerException if this record is invalid and therefore has no wrapped entry
     */
    public RedisStandaloneResponse send(Pipeline jedisPipelined, RedisTtl redisTTL) {
        return redisEntry.send(jedisPipelined, redisTTL);
    }

    /**
     * Delegates to the wrapped entry to execute this record's command on a cluster connection.
     *
     * @param jedisCluster the cluster connection the command is executed on
     * @param redisTTL the TTL strategy applied to the entry's key
     * @return the {@link RedisClusterResponse} produced by the wrapped {@link RedisEntry}
     * @throws NullPointerException if this record is invalid and therefore has no wrapped entry
     */
    public RedisClusterResponse send(JedisCluster jedisCluster, RedisTtl redisTTL) {
        return redisEntry.send(jedisCluster, redisTTL);
    }

    /**
     * Returns a debug representation combining the message metadata and the wrapped entry.
     *
     * <p>When the record is invalid and has no entry, the literal {@code NULL} is shown in place of
     * the entry description.</p>
     *
     * @return a human-readable string of the metadata and the wrapped entry (or {@code NULL})
     */
    @Override
    public String toString() {
        return String.format("Metadata %s %s", metadata, redisEntry != null ? redisEntry.toString() : "NULL");
    }
}
