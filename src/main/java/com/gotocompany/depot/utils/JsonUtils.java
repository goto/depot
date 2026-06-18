package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import org.json.JSONObject;

/**
 * Helper for building JSON objects from raw payloads, honoring the sink's string-mode setting.
 *
 * <p>Some Depot JSON sinks require every value to be represented as a string. This helper centralizes
 * that decision so JSON payloads are parsed consistently: when string mode is disabled the payload is
 * returned as parsed, and when it is enabled every top-level value is coerced to its string form.
 * Nested JSON objects are not supported in string mode.</p>
 */
public class JsonUtils {
    /**
     * Creates a json Object based on the configuration.
     * If String mode is enabled, it converts all the fields in string.
     *
     * <p>The {@code payload} bytes are first parsed into a {@link org.json.JSONObject}. When string
     * mode is disabled (see
     * {@link SinkConfig#getSinkConnectorSchemaJsonParserStringModeEnabled()}) that object is returned
     * unchanged. When string mode is enabled a new object is produced in which every top-level value
     * is converted to its {@link Object#toString()} form; JSON {@code null} values are dropped, and a
     * nested JSON object causes the conversion to fail because it cannot be flattened to a string.</p>
     *
     * @param config  Sink Configuration
     * @param payload Json Payload in byyes
     * @return Json object
     * @throws UnsupportedOperationException if string mode is enabled and a top-level value is itself a
     *     nested JSON object
     * @throws org.json.JSONException if {@code payload} does not contain a valid JSON object
     */
    public static JSONObject getJsonObject(SinkConfig config, byte[] payload) {
        JSONObject jsonObject = new JSONObject(new String(payload));
        if (!config.getSinkConnectorSchemaJsonParserStringModeEnabled()) {
            return jsonObject;
        }
        // convert to all objects to string
        JSONObject jsonWithStringValues = new JSONObject();
        jsonObject.keySet()
                .forEach(k -> {
                    Object value = jsonObject.get(k);
                    if (value instanceof JSONObject) {
                        throw new UnsupportedOperationException("nested json structure not supported yet");
                    }
                    if (JSONObject.NULL.equals(value)) {
                        return;
                    }
                    jsonWithStringValues.put(k, value.toString());
                });

        return jsonWithStringValues;
    }
}
