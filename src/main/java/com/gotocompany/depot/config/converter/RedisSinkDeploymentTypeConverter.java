package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link RedisSinkDeploymentType} constant.
 *
 * <p>This converter is registered against the Redis sink configuration through the Aeon Owner
 * {@code @ConverterClass} mechanism. It selects the topology of the target Redis deployment the
 * sink connects to, exposing the choice to Depot as a strongly typed enum rather than as a raw
 * {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link RedisSinkDeploymentType} constants
 * ({@code STANDALONE} and {@code CLUSTER}) and are matched case-insensitively.
 *
 * @see RedisSinkDeploymentType
 * @see Converter
 */
public class RedisSinkDeploymentTypeConverter implements Converter<RedisSinkDeploymentType> {
    /**
     * Converts the raw configuration value into the matching {@link RedisSinkDeploymentType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link RedisSinkDeploymentType#valueOf(String)}, which makes the lookup effectively
     * case-insensitive while still requiring an otherwise exact match of a declared constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link RedisSinkDeploymentType} constant
     * @return the {@link RedisSinkDeploymentType} constant whose name matches {@code input}
     *         ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public RedisSinkDeploymentType convert(Method method, String input) {
        return RedisSinkDeploymentType.valueOf(input.toUpperCase());
    }
}
