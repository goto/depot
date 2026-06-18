package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.json.JSONObject;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link JsonUtils#getJsonObject(SinkConfig, byte[])}, covering JSON parsing under both
 * string-mode settings.
 *
 * <p>The class runs with {@link MockitoJUnitRunner}, which injects a mock {@link SinkConfig} whose
 * string-mode flag is stubbed per test by {@link #setSinkConfigs(boolean)}. When string mode is
 * enabled every top-level value must be coerced to a string and nested objects are rejected; when it
 * is disabled the payload is parsed unchanged, including nested objects.
 */
@RunWith(MockitoJUnitRunner.class)
public class JsonUtilsTest {
    /**
     * Mock sink configuration whose JSON string-mode flag is stubbed per scenario.
     */
    @Mock
    private SinkConfig sinkConfig;

    /**
     * Stubs the mock {@link SinkConfig} to report the given JSON string-mode setting.
     *
     * @param stringModeEnabled the value returned by
     *                          {@link SinkConfig#getSinkConnectorSchemaJsonParserStringModeEnabled()}
     */
    void setSinkConfigs(boolean stringModeEnabled) {
        when(sinkConfig.getSinkConnectorSchemaJsonParserStringModeEnabled()).thenReturn(stringModeEnabled);
    }

    /**
     * Verifies that a flat JSON of string values parses unchanged when string mode is enabled.
     *
     * <p>With string mode on, parses a payload whose values are all strings and asserts the result is
     * similar to the original object, confirming already-string values are preserved.
     */
    @Test
    public void shouldParseSimpleJsonWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("name", "foo");
        expectedJson.put("num", "0371480");
        expectedJson.put("balance", "100");
        expectedJson.put("is_vip", "YES");
        byte[] payload = expectedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(sinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(expectedJson));
    }

    /**
     * Verifies that scalar values are coerced to strings when string mode is enabled.
     *
     * <p>Parses a payload containing an integer, a double and a boolean, and asserts each top-level
     * value is converted to its string form (for example {@code 100} becomes {@code "100"} and
     * {@code true} becomes {@code "true"}).
     */
    @Test
    public void shouldCastAllTypeToStringWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject originalJson = new JSONObject();
        originalJson.put("name", "foo");
        originalJson.put("num", new Integer(100));
        originalJson.put("balance", new Double(1000.21));
        originalJson.put("is_vip", Boolean.TRUE);
        byte[] payload = originalJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(sinkConfig, payload);
        JSONObject stringJson = new JSONObject();
        stringJson.put("name", "foo");
        stringJson.put("num", "100");
        stringJson.put("balance", "1000.21");
        stringJson.put("is_vip", "true");
        Assert.assertTrue(parsedJson.similar(stringJson));
    }

    /**
     * Verifies that a nested object is rejected when string mode is enabled.
     *
     * <p>Parses a payload whose value is itself a JSON object and asserts an
     * {@link UnsupportedOperationException} carrying the message
     * {@code "nested json structure not supported yet"} is thrown, since nested structures cannot be
     * flattened to strings.
     */
    @Test
    public void shouldThrowExceptionForNestedJsonWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject nestedJsonField = new JSONObject();
        nestedJsonField.put("name", "foo");
        nestedJsonField.put("num", "0371480");
        nestedJsonField.put("balance", "100");
        nestedJsonField.put("is_vip", "YES");
        JSONObject nestedJson = new JSONObject();
        nestedJson.put("ID", 1);
        nestedJson.put("nestedField", nestedJsonField);
        byte[] payload = nestedJson.toString().getBytes();
        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> JsonUtils.getJsonObject(sinkConfig, payload));
        assertEquals("nested json structure not supported yet", exception.getMessage());
    }

    /**
     * Verifies that a flat JSON parses with its native value types when string mode is disabled.
     *
     * <p>With string mode off, parses a payload containing an integer, a double and a boolean and
     * asserts the result is similar to the original object, confirming values keep their JSON types.
     */
    @Test
    public void shouldParseSimpleJsonWhenStringModeDisabled() {
        setSinkConfigs(false);
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("name", "foo");
        expectedJson.put("num", new Integer(100));
        expectedJson.put("balance", new Double(1000.21));
        expectedJson.put("is_vip", Boolean.TRUE);
        byte[] payload = expectedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(sinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(expectedJson));
    }

    /**
     * Verifies that nested objects are preserved when string mode is disabled.
     *
     * <p>With string mode off, parses a payload containing a nested object and asserts the result is
     * similar to the original, confirming nested structures are retained rather than rejected.
     */
    @Test
    public void shouldParseNestedJsonWhenStringModeDisabled() {
        setSinkConfigs(false);
        JSONObject nestedJsonField = new JSONObject();
        nestedJsonField.put("name", "foo");
        nestedJsonField.put("num", new Integer(100));
        nestedJsonField.put("balance", new Double(1000.21));
        nestedJsonField.put("is_vip", Boolean.TRUE);
        JSONObject nestedJson = new JSONObject();
        nestedJson.put("ID", 1);
        nestedJson.put("nestedField", nestedJsonField);
        byte[] payload = nestedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(sinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(nestedJson));
    }
}
