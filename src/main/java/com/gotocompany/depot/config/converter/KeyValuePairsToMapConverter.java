package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Owner {@link Converter} that parses a delimited list of {@code key=value} pairs into a
 * {@code Map<String, String>}.
 *
 * <p>The input is split into entries on the {@link #ENTRY_SEPARATOR} and each entry is split into a
 * key and a value on the {@link #KEY_VALUE_SEPARATOR}. The entries are gathered into a map; if the
 * same key appears more than once the last occurrence wins. A {@code null} or empty input yields an
 * empty map. Unlike some other converters in this package the keys and values are stored verbatim
 * and are not trimmed.
 *
 * @see Converter
 */
public class KeyValuePairsToMapConverter implements Converter<Map<String, String>> {

    /** Delimiter separating a key from its value within a single entry. */
    private static final String KEY_VALUE_SEPARATOR = "=";
    /** Delimiter separating consecutive key-value entries. */
    private static final String ENTRY_SEPARATOR = ",";

    /**
     * Parses the delimited {@code key=value} configuration string into a map.
     *
     * <p>An empty or {@code null} input returns an empty map. Otherwise the input is split on the
     * {@link #ENTRY_SEPARATOR}, each entry is split on the {@link #KEY_VALUE_SEPARATOR}, and the
     * resulting pairs are collected into a map. When duplicate keys are encountered the value from
     * the later entry replaces the earlier one.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param s      the raw property value, a comma-separated list of {@code key=value} pairs
     * @return a map of the parsed keys to their values; empty when {@code s} is {@code null} or empty
     * @throws ArrayIndexOutOfBoundsException if an entry does not contain the
     *         {@link #KEY_VALUE_SEPARATOR} and therefore has no value segment
     */
    @Override
    public Map<String, String> convert(Method method, String s) {
        if (StringUtils.isEmpty(s)) {
            return Collections.emptyMap();
        }
        return Stream.of(s.split(ENTRY_SEPARATOR))
                .map(entry -> entry.split(KEY_VALUE_SEPARATOR))
                .collect(Collectors.toMap(
                        entry -> entry[0],
                        entry -> entry[1],
                        (entry1, entry2) -> entry2
                ));
    }

}
