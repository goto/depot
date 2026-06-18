package com.gotocompany.depot.redis.ttl;


import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import com.gotocompany.depot.config.RedisSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;

/**
 * Factory that resolves the {@link RedisTtl} strategy from configuration.
 *
 * <p>Returns {@link NoRedisTtl} when {@code SINK_REDIS_TTL_TYPE} is {@link RedisSinkTtlType#DISABLE};
 * otherwise it validates the configured TTL value and returns {@link ExactTimeTtl} for
 * {@code EXACT_TIME} or {@link DurationTtl} for {@code DURATION}.</p>
 */
public class RedisTTLFactory {

    /**
     * Resolves the TTL strategy for the configured TTL type.
     *
     * <p>When the type is {@link RedisSinkTtlType#DISABLE} a {@link NoRedisTtl} is returned without
     * inspecting the value. Otherwise the configured value must be non-negative, and the type selects
     * the strategy: {@link RedisSinkTtlType#EXACT_TIME} yields an {@link ExactTimeTtl} (absolute Unix
     * time) and {@link RedisSinkTtlType#DURATION} yields a {@link DurationTtl} (relative seconds,
     * narrowed to an {@code int}).</p>
     *
     * @param redisSinkConfig the Redis sink configuration supplying the TTL type and value
     * @return the matching {@link RedisTtl} strategy
     * @throws ConfigurationException if the TTL value is negative, or if the TTL type is not one of
     *     the supported values
     */
    public static RedisTtl getTTl(RedisSinkConfig redisSinkConfig) {
        if (redisSinkConfig.getSinkRedisTtlType() == RedisSinkTtlType.DISABLE) {
            return new NoRedisTtl();
        }
        long redisTTLValue = redisSinkConfig.getSinkRedisTtlValue();
        if (redisTTLValue < 0) {
            throw new ConfigurationException("Provide a positive TTL value");
        }
        switch (redisSinkConfig.getSinkRedisTtlType()) {
            case EXACT_TIME:
                return new ExactTimeTtl(redisTTLValue);
            case DURATION:
                return new DurationTtl((int) redisTTLValue);
            default:
                throw new ConfigurationException("Not a valid TTL config");
        }
    }
}
