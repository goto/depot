package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RedisSinkDeploymentTypeConverter}, the Owner converter that maps a
 * configuration string to a {@link RedisSinkDeploymentType} enum constant.
 *
 * <p>A fresh converter is created in {@link #setup()} before each test. The cases assert that the
 * conversion is case-insensitive across the supported deployment types and that unsupported or empty
 * input raises an {@link IllegalArgumentException}.
 */
public class RedisSinkDeploymentTypeConverterTest {
    /**
     * Converter under test, recreated before each test by {@link #setup()}.
     */
    private RedisSinkDeploymentTypeConverter redisSinkDeploymentTypeConverter;

    /**
     * Creates a fresh {@link RedisSinkDeploymentTypeConverter} before each test.
     */
    @Before
    public void setup() {
        redisSinkDeploymentTypeConverter = new RedisSinkDeploymentTypeConverter();
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} maps lower-case
     * {@code "standalone"} to {@link RedisSinkDeploymentType#STANDALONE}.
     *
     * <p>Given the input {@code "standalone"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDeploymentType#STANDALONE}.
     */
    @Test
    public void shouldReturnStandaloneTypeFromLowerCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "standalone");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} maps upper-case
     * {@code "STANDALONE"} to {@link RedisSinkDeploymentType#STANDALONE}.
     *
     * <p>Given the input {@code "STANDALONE"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDeploymentType#STANDALONE}.
     */
    @Test
    public void shouldReturnStandaloneTypeFromUpperCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "STANDALONE");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} maps mixed-case
     * {@code "stANdAlOne"} to {@link RedisSinkDeploymentType#STANDALONE}.
     *
     * <p>Given the input {@code "stANdAlOne"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDeploymentType#STANDALONE}.
     */
    @Test
    public void shouldReturnStandaloneTypeFromMixedCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "stANdAlOne");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.STANDALONE));
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} maps upper-case
     * {@code "CLUSTER"} to {@link RedisSinkDeploymentType#CLUSTER}.
     *
     * <p>Given the input {@code "CLUSTER"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDeploymentType#CLUSTER}.
     */
    @Test
    public void shouldReturnClusterTypeFromUpperCaseInput() {
        RedisSinkDeploymentType redisSinkDeploymentType = redisSinkDeploymentTypeConverter.convert(null, "CLUSTER");
        Assert.assertTrue(redisSinkDeploymentType.equals(RedisSinkDeploymentType.CLUSTER));
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} rejects an empty input string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkDeploymentTypeConverter.convert(null, "");
    }

    /**
     * Verifies that {@link RedisSinkDeploymentTypeConverter#convert} rejects an unrecognized value.
     *
     * <p>Given the input {@code "INVALID"}, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkDeploymentTypeConverter.convert(null, "INVALID");
    }
}
