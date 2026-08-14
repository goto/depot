package com.gotocompany.depot.config.converter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

/**
 * Unit tests for {@link RangeToHashMapConverter}, the Owner converter that expands one or more
 * inclusive integer ranges into a {@code Map<Integer, Boolean>}.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link RangeToHashMapConverter#convert}
 * with a {@code null} accessor method, asserting either the expanded set of keys or, via the
 * {@link ExpectedException} rule, the {@link IllegalArgumentException} raised for malformed ranges.
 */
public class RangeToHashMapConverterTest {

    /**
     * JUnit rule used to assert the type and message of exceptions raised by invalid range input.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Verifies that {@link RangeToHashMapConverter#convert} expands a single inclusive range into
     * its member integers.
     *
     * <p>Given the input {@code "100-103"}, when {@code convert} is invoked, then the resulting
     * map's keys are exactly {@code 100, 101, 102, 103}.
     */
    @Test
    public void shouldConvertRangeToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103");
        assertArrayEquals(new Integer[]{100, 101, 102, 103}, actualHashedRanges.keySet().toArray());
    }

    /**
     * Verifies that {@link RangeToHashMapConverter#convert} expands several comma-separated ranges.
     *
     * <p>Given the input {@code "100-103,200-203"}, when {@code convert} is invoked, then the
     * resulting map's keys are exactly {@code 100, 101, 102, 103, 200, 201, 202, 203}.
     */
    @Test
    public void shouldConvertRangesToHashMap() {
        Map<Integer, Boolean> actualHashedRanges = new RangeToHashMapConverter().convert(null, "100-103,200-203");
        assertArrayEquals(new Integer[]{100, 101, 102, 103, 200, 201, 202, 203}, actualHashedRanges.keySet().toArray());
    }

    /**
     * Verifies that {@link RangeToHashMapConverter#convert} rejects an entry that is a bare value
     * rather than a range.
     *
     * <p>Given the input {@code "100,200-203"} whose first token {@code "100"} lacks a {@code "-"}
     * separator, when {@code convert} is invoked, then an {@link IllegalArgumentException} is thrown
     * with the message {@code "input value '100' is not a valid range"}.
     */
    @Test
    public void shouldThrowExceptionIfConfigNotRangeValues() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value '100' is not a valid range");
        new RangeToHashMapConverter().convert(null, "100,200-203");
    }

    /**
     * Verifies that {@link RangeToHashMapConverter#convert} rejects a non-numeric, non-range token.
     *
     * <p>Given the input {@code "string"}, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown with the message
     * {@code "input value 'string' is not a valid range"}.
     */
    @Test
    public void shouldThrowExceptionIfConfigIsInvalid() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("input value 'string' is not a valid range");
        new RangeToHashMapConverter().convert(null, "string");
    }
}
