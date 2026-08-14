package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Owner {@link Converter} that parses a delimited list of HTTP headers into a
 * {@code Map<String, String>}.
 *
 * <p>The input is split into entries on the {@link #ELEMENT_SEPARATOR} and each non-blank entry is
 * split into a name and a value on the {@link #VALUE_SEPARATOR}, producing a map of header names to
 * header values. Entries that are blank after trimming are ignored; the header names and values
 * themselves are taken verbatim from the split and are not trimmed.
 *
 * @see Converter
 */
public class HttpHeaderConverter implements Converter<Map<String, String>> {

    /** Delimiter separating consecutive header entries. */
    private static final String ELEMENT_SEPARATOR = ",";

    /** Delimiter separating a header name from its value. */
    private static final String VALUE_SEPARATOR = ":";

    /**
     * Parses the delimited header configuration string into a map of header names to values.
     *
     * <p>The input is split on the {@link #ELEMENT_SEPARATOR}; entries that are empty after trimming
     * are discarded, and each remaining entry is split on the {@link #VALUE_SEPARATOR} with the first
     * segment used as the header name and the second as the header value.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, a comma-separated list of {@code name:value} headers
     * @return a map of header names to header values
     * @throws IllegalStateException          if two entries resolve to the same header name
     * @throws ArrayIndexOutOfBoundsException if an entry does not contain the {@link #VALUE_SEPARATOR}
     *         and therefore has no value segment
     * @throws NullPointerException           if {@code input} is {@code null}
     */
    @Override
    public Map<String, String> convert(Method method, String input) {
        return Arrays.stream(input.split(ELEMENT_SEPARATOR))
                .filter(headerKeyValue -> !headerKeyValue.trim().isEmpty())
                .collect(Collectors.toMap(headerKeyValue -> headerKeyValue.split(VALUE_SEPARATOR)[0],
                        headerKeyValue -> headerKeyValue.split(VALUE_SEPARATOR)[1]));
    }
}
