package com.gotocompany.depot.redis.enums;

/**
 * Strategy for expiring Redis keys written by the sink, selected by {@code SINK_REDIS_TTL_TYPE}.
 *
 * <p>It controls which {@link com.gotocompany.depot.redis.ttl.RedisTtl} implementation
 * {@link com.gotocompany.depot.redis.ttl.RedisTTLFactory} builds from the configured TTL value.</p>
 */
public enum RedisSinkTtlType {
    /**
     * Expires each key at an absolute Unix timestamp (in seconds) via {@code EXPIREAT}.
     */
    EXACT_TIME,
    /**
     * Expires each key after a relative number of seconds via {@code EXPIRE}.
     */
    DURATION,
    /**
     * Disables key expiry; no TTL command is issued.
     */
    DISABLE
}
