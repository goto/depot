package com.gotocompany.depot.config.converter;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;


/**
 * Unit tests for {@link LabelMapConverter}, the Owner converter that parses a comma-separated list
 * of {@code key=value} labels into a {@code Map<String, String>}.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link LabelMapConverter#convert} with a
 * {@code null} accessor method, asserting the resulting map (empty for blank input).
 */
public class LabelMapConverterTest {

    /**
     * Verifies that {@link LabelMapConverter#convert} returns an empty map for an empty input
     * string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then the result equals an empty
     * map.
     */
    @Test
    public void shouldConvertToEmptyMap() {
        LabelMapConverter converter = new LabelMapConverter();
        Assert.assertEquals(Collections.emptyMap(), converter.convert(null, ""));
    }

    /**
     * Verifies that {@link LabelMapConverter#convert} parses multiple {@code key=value} labels into
     * a map.
     *
     * <p>Given the input {@code "a=b,c=d,test=testing"}, when {@code convert} is invoked, then the
     * result equals a map containing the entries {@code a=b}, {@code c=d} and {@code test=testing}.
     */
    @Test
    public void shouldConvertToMap() {
        LabelMapConverter converter = new LabelMapConverter();
        Assert.assertEquals(new HashMap<String, String>() {{
            put("a", "b");
            put("c", "d");
            put("test", "testing");
        }}, converter.convert(null, "a=b,c=d,test=testing"));
    }
}
