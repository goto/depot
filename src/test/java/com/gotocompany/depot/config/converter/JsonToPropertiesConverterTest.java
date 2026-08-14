package com.gotocompany.depot.config.converter;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link JsonToPropertiesConverter}, the Owner converter that parses a JSON object
 * into a {@link Properties} instance, recursing into nested objects.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link JsonToPropertiesConverter#convert}
 * with a {@code null} accessor method. The cases cover flat and nested JSON, rejection of duplicate
 * mapped values (including across nested objects), and the {@code null} result returned for empty or
 * {@code null} input.
 */
public class JsonToPropertiesConverterTest {

    /**
     * JUnit rule for declaring expected exceptions. The exception scenarios in this fixture instead
     * use {@link Assert#assertThrows}, so this rule is left in its default no-expectation state.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} parses a flat JSON object into
     * matching properties.
     *
     * <p>Given a JSON object with three string fields, when {@code convert} is invoked, then the
     * returned {@link Properties} has three entries mapping {@code order_number},
     * {@code event_timestamp} and {@code driver_id} to {@code "ORDER_NUMBER"}, {@code "TIMESTAMP"}
     * and {@code "DRIVER_ID"} respectively.
     */
    @Test
    public void shouldConvertJSONConfigToProperties() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"driver_id\":\"DRIVER_ID\"}";

        Properties properties = new JsonToPropertiesConverter().convert(null, json);

        assertEquals(3, properties.size());
        assertEquals("ORDER_NUMBER", properties.get("order_number"));
        assertEquals("TIMESTAMP", properties.get("event_timestamp"));
        assertEquals("DRIVER_ID", properties.get("driver_id"));
    }

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} rejects duplicate mapped values in a
     * flat object.
     *
     * <p>Given a JSON object in which two fields both map to {@code "TIMESTAMP"}, when
     * {@code convert} is invoked, then an {@link IllegalArgumentException} is thrown reporting the
     * duplicate {@code [TIMESTAMP]} for {@code SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING}.
     */
    @Test
    public void shouldValidateJsonConfigForDuplicates() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"driver_id\":\"TIMESTAMP\"}";
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class, () -> new JsonToPropertiesConverter().convert(null, json));
        Assert.assertEquals("duplicates found in SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING for : [TIMESTAMP]", e.getMessage());
    }

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} detects duplicate mapped values across
     * nested objects.
     *
     * <p>Given a JSON object whose top-level and nested fields collectively repeat
     * {@code "ORDER_NUMBER"} and {@code "TIMESTAMP"}, when {@code convert} is invoked, then an
     * {@link IllegalArgumentException} is thrown whose message names both duplicated values, in
     * either order.
     */
    @Test
    public void shouldValidateJsonConfigForDuplicatesInNestedJsons() {
        String json = "{\"order_number\":\"ORDER_NUMBER\",\"event_timestamp\":\"TIMESTAMP\",\"nested\":{\"1\":\"TIMESTAMP\",\"2\":\"ORDER_NUMBER\"}}";
        IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class, () -> new JsonToPropertiesConverter().convert(null, json));
        String message = e.getMessage();
        String[] actualMessage = (message.split(" : "));
        Assert.assertEquals("duplicates found in SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING for", actualMessage[0]);
        Assert.assertTrue("[ORDER_NUMBER, TIMESTAMP]" .equals(actualMessage[1]) || "[TIMESTAMP, ORDER_NUMBER]" .equals(actualMessage[1]));
    }

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} preserves nesting when parsing a
     * nested JSON object.
     *
     * <p>Given a JSON object containing a nested {@code order_id} object alongside a scalar field,
     * when {@code convert} is invoked, then the returned {@link Properties} contains a nested
     * {@link Properties} under {@code order_id} (holding {@code order_number}, {@code order_url} and
     * {@code order_details}) and the scalar {@code nested_order_details} mapped to
     * {@code "NUMBER_FIELDS"}.
     */
    @Test
    public void shouldConvertNestedJSONToNestedProperties() {
        String json = "{\"order_id\":{\"order_number\":\"ORDER_NUMBER\",\"order_url\":\"ORDER_URL\",\"order_details\":\"ORDER_DETAILS\"},\"nested_order_details\":\"NUMBER_FIELDS\"}";

        Properties actualProperties = new JsonToPropertiesConverter().convert(null, json);

        Properties expectedNestedProperties = new Properties();
        expectedNestedProperties.put("order_number", "ORDER_NUMBER");
        expectedNestedProperties.put("order_url", "ORDER_URL");
        expectedNestedProperties.put("order_details", "ORDER_DETAILS");

        Properties expectedProperties = new Properties();
        expectedProperties.put("order_id", expectedNestedProperties);
        expectedProperties.put("nested_order_details", "NUMBER_FIELDS");

        assertEquals(actualProperties, expectedProperties);
    }

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} returns {@code null} for an empty
     * string.
     *
     * <p>Given an empty string, when {@code convert} is invoked, then the result is {@code null}.
     */
    @Test
    public void shouldNotProcessEmptyStringAsProperties() {
        String json = "";
        Properties actualProperties = new JsonToPropertiesConverter().convert(null, json);
        assertNull(actualProperties);
    }

    /**
     * Verifies that {@link JsonToPropertiesConverter#convert} returns {@code null} for {@code null}
     * input.
     *
     * <p>Given a {@code null} input, when {@code convert} is invoked, then the result is
     * {@code null}.
     */
    @Test
    public void shouldNotProcessNullStringAsProperties() {
        Properties actualProperties = new JsonToPropertiesConverter().convert(null, null);
        assertNull(actualProperties);
    }
}
