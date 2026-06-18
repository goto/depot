package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Owner {@link Converter} that parses a delimited list of {@code key=value} pairs into a
 * {@code Map<String, String>} of MaxCompute (ODPS) global settings.
 *
 * <p>The input is split into entries on the {@link #CONFIG_SEPARATOR} and each entry is split into a
 * key and a value on the {@link #KEY_VALUE_SEPARATOR}. Every entry must contain exactly one
 * separator; keys and values are trimmed before being stored. A {@code null} or blank input yields
 * an empty map. The resulting map is applied as global hints/settings on the MaxCompute client.
 *
 * @see Converter
 */
public class MaxComputeOdpsGlobalSettingsConverter implements Converter<Map<String, String>> {

    /** Delimiter separating consecutive key-value settings. */
    private static final String CONFIG_SEPARATOR = ",";
    /** Delimiter separating a key from its value within a single setting. */
    private static final String KEY_VALUE_SEPARATOR = "=";

    /**
     * Parses the delimited {@code key=value} configuration string into a settings map.
     *
     * <p>If the input is {@code null} or blank an empty map is returned. Otherwise the input is split
     * on the {@link #CONFIG_SEPARATOR}; each resulting pair is split on the
     * {@link #KEY_VALUE_SEPARATOR} and must yield exactly two parts, which are trimmed and stored as
     * a key and a value.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param s      the raw property value, a comma-separated list of {@code key=value} settings
     * @return a map of trimmed setting keys to trimmed setting values; empty when {@code s} is
     *         {@code null} or blank
     * @throws IllegalArgumentException if any entry does not split into exactly one key and one value
     */
    @Override
    public Map<String, String> convert(Method method, String s) {
        Map<String, String> settings = new HashMap<>();
        if (Objects.isNull(s) || StringUtils.isEmpty(s.trim())) {
            return settings;
        }
        String[] pairs = s.split(CONFIG_SEPARATOR);
        for (String pair : pairs) {
            String[] keyValue = pair.split(KEY_VALUE_SEPARATOR);
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid key-value pair: " + pair);
            }
            settings.put(keyValue[0].trim(), keyValue[1].trim());
        }
        return settings;
    }

}
