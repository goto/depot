package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RedisSinkTtlTypeConverter}, the Owner converter that maps a configuration
 * string to a {@link RedisSinkTtlType} enum constant.
 *
 * <p>A fresh converter is created in {@link #setUp()} before each test. The cases assert that the
 * conversion is case-insensitive across the supported TTL types and that unsupported or empty input
 * raises an {@link IllegalArgumentException}.
 */
public class RedisSinkTtlTypeConverterTest {
    /**
     * Converter under test, recreated before each test by {@link #setUp()}.
     */
    private RedisSinkTtlTypeConverter redisSinkTtlTypeConverter;

    /**
     * Creates a fresh {@link RedisSinkTtlTypeConverter} before each test.
     */
    @Before
    public void setUp() {
        redisSinkTtlTypeConverter = new RedisSinkTtlTypeConverter();
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} maps lower-case {@code "exact_time"}
     * to {@link RedisSinkTtlType#EXACT_TIME}.
     *
     * <p>Given the input {@code "exact_time"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkTtlType#EXACT_TIME}.
     */
    @Test
    public void shouldReturnExactTimeTypeFromLowerCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "exact_time");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} maps upper-case {@code "EXACT_TIME"}
     * to {@link RedisSinkTtlType#EXACT_TIME}.
     *
     * <p>Given the input {@code "EXACT_TIME"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkTtlType#EXACT_TIME}.
     */
    @Test
    public void shouldReturnExactTimeTypeFromUpperCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "EXACT_TIME");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} maps mixed-case {@code "eXAct_TiMe"}
     * to {@link RedisSinkTtlType#EXACT_TIME}.
     *
     * <p>Given the input {@code "eXAct_TiMe"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkTtlType#EXACT_TIME}.
     */
    @Test
    public void shouldReturnExactTimeTypeFromMixedCaseInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "eXAct_TiMe");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.EXACT_TIME));
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} maps {@code "DISABLE"} to
     * {@link RedisSinkTtlType#DISABLE}.
     *
     * <p>Given the input {@code "DISABLE"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkTtlType#DISABLE}.
     */
    @Test
    public void shouldReturnDisableTypeFromInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "DISABLE");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.DISABLE));
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} maps {@code "DURATION"} to
     * {@link RedisSinkTtlType#DURATION}.
     *
     * <p>Given the input {@code "DURATION"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkTtlType#DURATION}.
     */
    @Test
    public void shouldReturnDurationTypeFromInput() {
        RedisSinkTtlType redisSinkTtlType = redisSinkTtlTypeConverter.convert(null, "DURATION");
        Assert.assertTrue(redisSinkTtlType.equals(RedisSinkTtlType.DURATION));
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} rejects an empty input string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkTtlTypeConverter.convert(null, "");
    }

    /**
     * Verifies that {@link RedisSinkTtlTypeConverter#convert} rejects an unrecognized value.
     *
     * <p>Given the input {@code "INVALID"}, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkTtlTypeConverter.convert(null, "INVALID");
    }
}
