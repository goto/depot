package com.gotocompany.depot.redis.enums;

/**
 * Supported shapes for the data the Redis sink writes, selected by {@code SINK_REDIS_DATA_TYPE}.
 *
 * <p>The chosen value determines which Redis command each record maps to and which
 * {@link com.gotocompany.depot.redis.parsers.RedisEntryParser} is built to produce the entries.</p>
 */
public enum RedisSinkDataType {
    /**
     * Appends the configured field's value to a Redis list using {@code LPUSH}.
     */
    LIST,
    /**
     * Sets one or more fields of a Redis hash using {@code HSET}, according to the configured
     * field-to-column mapping.
     */
    HASHSET,
    /**
     * Stores a single key/value pair using {@code SET}, taking the value from the configured field.
     */
    KEYVALUE,
}
