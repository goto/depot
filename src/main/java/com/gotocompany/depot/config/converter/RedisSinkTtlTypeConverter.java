package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link RedisSinkTtlType} constant.
 *
 * <p>This converter is registered against the Redis sink configuration through the Aeon Owner
 * {@code @ConverterClass} mechanism. It selects how the time-to-live applied to Redis entries is
 * interpreted, exposing the choice to Depot as a strongly typed enum rather than as a raw
 * {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link RedisSinkTtlType} constants
 * ({@code EXACT_TIME}, {@code DURATION} and {@code DISABLE}) and are matched case-insensitively.
 *
 * @see RedisSinkTtlType
 * @see Converter
 */
public class RedisSinkTtlTypeConverter implements Converter<RedisSinkTtlType> {
    /**
     * Converts the raw configuration value into the matching {@link RedisSinkTtlType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link RedisSinkTtlType#valueOf(String)}, which makes the lookup effectively
     * case-insensitive while still requiring an otherwise exact match of a declared constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link RedisSinkTtlType} constant
     * @return the {@link RedisSinkTtlType} constant whose name matches {@code input} ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public RedisSinkTtlType convert(Method method, String input) {
        return RedisSinkTtlType.valueOf(input.toUpperCase());
    }
}
