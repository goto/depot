package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


/**
 * Unit tests for {@link ConverterUtils}, the helper that splits a comma-separated list of
 * {@code key=value} entries into a list of {@link Tuple} pairs.
 *
 * <p>The tests exercise {@link ConverterUtils#convertToList(String)} with both an empty string and a
 * populated list, asserting the resulting tuples and their order.
 */
public class ConverterUtilsTest {

    /**
     * Verifies that {@link ConverterUtils#convertToList(String)} yields an empty list for empty
     * input.
     *
     * <p>Given an empty string, when {@code convertToList} is invoked, then the returned list has
     * size zero.
     */
    @Test
    public void testConvertEmpty() {
        List<Tuple<String, String>> tuples = ConverterUtils.convertToList("");
        Assert.assertEquals(0, tuples.size());
    }

    /**
     * Verifies that {@link ConverterUtils#convertToList(String)} parses multiple {@code key=value}
     * entries into ordered tuples.
     *
     * <p>Given the input {@code "a=b,c=d"}, when {@code convertToList} is invoked, then the returned
     * list contains exactly two tuples, {@code (a, b)} followed by {@code (c, d)}, preserving input
     * order.
     */
    @Test
    public void testConvert() {
        List<Tuple<String, String>> tuples = ConverterUtils.convertToList("a=b,c=d");
        Assert.assertEquals(2, tuples.size());
        Assert.assertEquals(new Tuple<>("a", "b"), tuples.get(0));
        Assert.assertEquals(new Tuple<>("c", "d"), tuples.get(1));
    }
}
