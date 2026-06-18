package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import org.gradle.internal.impldep.org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RedisSinkDataTypeConverter}, the Owner converter that maps a configuration
 * string to a {@link RedisSinkDataType} enum constant.
 *
 * <p>A fresh converter is created in {@link #setUp()} before each test. The cases assert that the
 * conversion is case-insensitive across the supported data types and that unsupported or empty input
 * raises an {@link IllegalArgumentException}.
 */
public class RedisSinkDataTypeConverterTest {

    /**
     * Converter under test, recreated before each test by {@link #setUp()}.
     */
    private RedisSinkDataTypeConverter redisSinkDataTypeConverter;

    /**
     * Creates a fresh {@link RedisSinkDataTypeConverter} before each test.
     */
    @Before
    public void setUp() {
        redisSinkDataTypeConverter = new RedisSinkDataTypeConverter();
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} maps lower-case {@code "list"} to
     * {@link RedisSinkDataType#LIST}.
     *
     * <p>Given the input {@code "list"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDataType#LIST}.
     */
    @Test
    public void shouldReturnListSinkTypeFromLowerCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "list");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} maps upper-case {@code "LIST"} to
     * {@link RedisSinkDataType#LIST}.
     *
     * <p>Given the input {@code "LIST"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDataType#LIST}.
     */
    @Test
    public void shouldReturnListSinkTypeFromUpperCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LIST");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} maps mixed-case {@code "LiSt"} to
     * {@link RedisSinkDataType#LIST}.
     *
     * <p>Given the input {@code "LiSt"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDataType#LIST}.
     */
    @Test
    public void shouldReturnListSinkTypeFromMixedCaseInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "LiSt");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.LIST));
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} maps {@code "hashset"} to
     * {@link RedisSinkDataType#HASHSET}.
     *
     * <p>Given the input {@code "hashset"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDataType#HASHSET}.
     */
    @Test
    public void shouldReturnHashSetSinkTypeFromInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "hashset");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.HASHSET));
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} maps {@code "keyvalue"} to
     * {@link RedisSinkDataType#KEYVALUE}.
     *
     * <p>Given the input {@code "keyvalue"}, when {@code convert} is invoked, then the result is
     * {@link RedisSinkDataType#KEYVALUE}.
     */
    @Test
    public void shouldReturnKeyValueSinkTypeFromInput() {
        RedisSinkDataType redisSinkDataType = redisSinkDataTypeConverter.convert(null, "keyvalue");
        Assert.assertTrue(redisSinkDataType.equals(RedisSinkDataType.KEYVALUE));
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} rejects an empty input string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnEmptyArgument() {
        redisSinkDataTypeConverter.convert(null, "");
    }

    /**
     * Verifies that {@link RedisSinkDataTypeConverter#convert} rejects an unrecognized value.
     *
     * <p>Given the input {@code "INVALID"}, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnInvalidArgument() {
        redisSinkDataTypeConverter.convert(null, "INVALID");
    }

}
