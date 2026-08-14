package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility helpers shared by the configuration {@link org.aeonbits.owner.Converter} implementations
 * in this package for turning delimited {@code key=value} strings into {@link Tuple} pairs.
 *
 * <p>The parsing grammar is described by the separators declared here: entries are delimited by the
 * {@link #ELEMENT_SEPARATOR} and the key and value within an entry are delimited by {@code "="}.
 * Parsed values are trimmed and capped in length so that downstream consumers (such as resource
 * label stores) receive well-formed values.
 *
 * @see Tuple
 * @see LabelMapConverter
 */
public class ConverterUtils {
    /** Delimiter that separates consecutive entries within a configuration value. */
    public static final String ELEMENT_SEPARATOR = ",";
    /** Delimiter that separates a key from its value within a single entry. */
    private static final String VALUE_SEPARATOR = "=";
    /** Maximum number of characters retained for a parsed value; longer values are truncated. */
    private static final int MAX_LENGTH = 63;

    /**
     * Parses a delimited {@code key=value} string into a list of trimmed key-value tuples.
     *
     * <p>The input is split into chunks on the {@link #ELEMENT_SEPARATOR} (retaining trailing empty
     * chunks), and each chunk is split on the {@code "="} value separator. A chunk is skipped when it
     * contains no value separator or when its key is empty after trimming. Both the key and the value
     * are trimmed, and the value is truncated to at most {@link #MAX_LENGTH} characters before the
     * resulting {@link Tuple} is added to the list.
     *
     * @param input the raw delimited string of {@code key=value} entries
     * @return a list of {@link Tuple} pairs, one per valid entry; entries without a value separator or
     *         with an empty key are omitted and over-long values are truncated
     * @throws NullPointerException if {@code input} is {@code null}
     */
    public static List<Tuple<String, String>> convertToList(String input) {
        List<Tuple<String, String>> result = new ArrayList<>();
        String[] chunks = input.split(ELEMENT_SEPARATOR, -1);
        for (String chunk : chunks) {
            String[] entry = chunk.split(VALUE_SEPARATOR, -1);
            if (entry.length <= 1) {
                continue;
            }
            String key = entry[0].trim();
            if (key.isEmpty()) {
                continue;
            }

            String value = entry[1].trim();
            value = value.length() > MAX_LENGTH ? value.substring(0, MAX_LENGTH) : value;
            result.add(new Tuple<>(key, value));
        }
        return result;
    }
}
