package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.TupleString;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ConfToListConverter}, the Owner converter that parses a single
 * {@code key=value} entry into a {@link TupleString}.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link ConfToListConverter#convert} with a
 * {@code null} accessor method, asserting either the parsed {@link TupleString} or {@code null} for
 * empty input.
 */
public class ConfToListConverterTest {

    /**
     * Verifies that {@link ConfToListConverter#convert} returns {@code null} for an empty input
     * string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then the result is {@code null},
     * signalling that no tuple is produced.
     */
    @Test
    public void shouldReturnNullForEmpty() {
        ConfToListConverter converter = new ConfToListConverter();
        Assert.assertNull(converter.convert(null, ""));
    }

    /**
     * Verifies that {@link ConfToListConverter#convert} parses a {@code key=value} entry into a
     * {@link TupleString}.
     *
     * <p>Given the input {@code "a=b"}, when {@code convert} is invoked, then the result equals a
     * {@link TupleString} whose first element is {@code "a"} and whose second element is {@code "b"}.
     */
    @Test
    public void shouldCovertToTuple() {
        ConfToListConverter converter = new ConfToListConverter();
        Assert.assertEquals(new TupleString("a", "b"), converter.convert(null, "a=b"));
    }
}
