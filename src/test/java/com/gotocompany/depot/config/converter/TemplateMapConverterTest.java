package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.exception.InvalidTemplateException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link TemplateMapConverter}, the Owner converter that parses a JSON object of
 * template strings into a {@code Map<Template, Template>}.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link TemplateMapConverter#convert} with
 * a {@code null} accessor method. The cases cover empty, blank and {@code null} input (all of which
 * yield an empty map) as well as single- and multi-entry JSON objects whose keys and values are
 * compiled into {@link Template} instances.
 */
public class TemplateMapConverterTest {

    /**
     * Verifies that {@link TemplateMapConverter#convert} returns an empty map for an empty JSON
     * object.
     *
     * <p>Given the input {@code "{}"}, when {@code convert} is invoked, then the result equals an
     * empty map.
     */
    @Test
    public void shouldConvertToEmptyMapIfInputIsEmpty() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, "{}"));
    }

    /**
     * Verifies that {@link TemplateMapConverter#convert} returns an empty map for an empty string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then the result equals an empty
     * map.
     */
    @Test
    public void shouldConvertToEmptyMapIfInputIsEmptyString() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, ""));
    }

    /**
     * Verifies that {@link TemplateMapConverter#convert} returns an empty map for {@code null}
     * input.
     *
     * <p>Given a {@code null} input, when {@code convert} is invoked, then the result equals an
     * empty map.
     */
    @Test
    public void shouldConvertToEmptyMapIfInputIsNull() {
        TemplateMapConverter converter = new TemplateMapConverter();
        assertEquals(Collections.emptyMap(), converter.convert(null, null));
    }

    /**
     * Verifies that {@link TemplateMapConverter#convert} parses a single-entry JSON object into a
     * template map.
     *
     * <p>Given a JSON object mapping {@code "key-%s,order_number"} to
     * {@code "message-%s,service_type"}, when {@code convert} is invoked, then the single produced
     * entry has a key equal to the {@link Template} compiled from {@code "key-%s,order_number"} and
     * a value equal to the {@link Template} compiled from {@code "message-%s,service_type"}.
     *
     * @throws InvalidTemplateException if constructing the expected {@link Template} fixtures fails
     */
    @Test
    public void shouldConvertToTemplateMap() throws InvalidTemplateException {
        TemplateMapConverter converter = new TemplateMapConverter();
        Template templateKey = new Template("key-%s,order_number");
        Template templateValue = new Template("message-%s,service_type");

        Map<Template, Template> templateMap = converter.convert(null, "{\"key-%s,order_number\":\"message-%s,service_type\"}");
        templateMap.forEach((k, v) -> {
                    assertEquals(templateKey, k);
                    assertEquals(templateValue, v);
                }
        );
    }

    /**
     * Verifies that {@link TemplateMapConverter#convert} parses a multi-entry JSON object into a
     * template map.
     *
     * <p>Given a JSON object with two key-value template pairs, when {@code convert} is invoked,
     * then the result equals a map containing both corresponding {@link Template}-to-{@link Template}
     * entries.
     *
     * @throws InvalidTemplateException if constructing the expected {@link Template} fixtures fails
     */
    @Test
    public void shouldConvertMultipleTemplateToTemplateMap() throws InvalidTemplateException {
        TemplateMapConverter converter = new TemplateMapConverter();
        Map<Template, Template> expectedTemplateMap = new HashMap<>();
        expectedTemplateMap.put(new Template("key1-%s,order_number"), new Template("message1-%s,service_type"));
        expectedTemplateMap.put(new Template("key2-%s,order_number"), new Template("message2-%s,service_type"));

        Map<Template, Template> templateMap = converter.convert(
                null,
                "{\"key1-%s,order_number\":\"message1-%s,service_type\",\"key2-%s,order_number\":\"message2-%s,service_type\"}");
        assertEquals(expectedTemplateMap, templateMap);
    }
}
