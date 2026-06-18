package com.gotocompany.depot.config.converter;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;


/**
 * Owner {@link org.aeonbits.owner.Converter} that parses a JSON object string into a
 * {@link Properties} instance, supporting arbitrarily nested objects.
 *
 * <p>Top-level string values are stored directly, while nested JSON objects are converted into
 * nested {@link Properties} values, allowing hierarchical configuration to be expressed as JSON.
 * After parsing, the flattened set of string values is validated so that none are duplicated; this
 * is used, for example, by the Redis sink's hash-set field-to-column mapping where each mapped
 * column must be unique.
 *
 * <p>A {@code null} or empty input produces a {@code null} result.
 *
 * @see org.aeonbits.owner.Converter
 */
public class JsonToPropertiesConverter implements org.aeonbits.owner.Converter<Properties> {
    /** Shared, thread-safe Gson instance used to deserialize the JSON input. */
    private static final Gson GSON = new Gson();

    /**
     * Parses the JSON object input into a (possibly nested) {@link Properties} instance.
     *
     * <p>If the input is {@code null} or empty, {@code null} is returned. Otherwise the input is
     * deserialized into a {@code Map<String, Object>} using Gson, transformed into a
     * {@link Properties} structure by {@link #getProperties(Map)}, and validated for duplicate values
     * by {@link #validate(Properties)} before being returned.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, expected to be a JSON object, or {@code null}/empty
     * @return the parsed {@link Properties}, or {@code null} when {@code input} is {@code null} or empty
     * @throws com.google.gson.JsonSyntaxException if {@code input} is not well-formed JSON
     * @throws IllegalArgumentException            if duplicate values are detected across the parsed
     *                                             properties
     */
    @Override
    public Properties convert(Method method, String input) {
        if (Strings.isNullOrEmpty(input)) {
            return null;
        }
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> m = GSON.fromJson(input, type);
        Properties properties = getProperties(m);
        validate(properties);
        return properties;
    }

    /**
     * Recursively converts a parsed JSON map into a {@link Properties} structure.
     *
     * <p>Each entry is inspected by runtime type: {@link String} values are stored directly under
     * their key, while {@link Map} values are recursively converted into a nested {@link Properties}
     * instance and stored under their key. Values of any other type are ignored.
     *
     * @param inputMap the map produced by deserializing the JSON input
     * @return a {@link Properties} instance mirroring the structure of {@code inputMap}, with nested
     *         maps represented as nested {@link Properties} values
     */
    private Properties getProperties(Map<String, Object> inputMap) {
        Properties properties = new Properties();
        for (String key : inputMap.keySet()) {
            Object value = inputMap.get(key);
            if (value instanceof String) {
                properties.put(key, value);
            } else if (value instanceof Map) {
                properties.put(key, getProperties((Map) value));
            }
        }
        return properties;
    }

    /**
     * Validates that no string value occurs more than once across the (possibly nested) properties.
     *
     * <p>All string values are flattened by {@link #flattenValues(Properties)} and accumulated into a
     * {@link DuplicateFinder}; if any duplicates are found an {@link IllegalArgumentException} is
     * raised that lists the offending values.
     *
     * @param properties the parsed properties to validate
     * @throws IllegalArgumentException if one or more string values are duplicated
     */
    private void validate(Properties properties) {
        DuplicateFinder duplicateFinder = flattenValues(properties)
                .collect(DuplicateFinder::new, DuplicateFinder::accept, DuplicateFinder::combine);
        if (duplicateFinder.duplicates.size() > 0) {
            throw new IllegalArgumentException("duplicates found in SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING for : " + duplicateFinder.duplicates);
        }
    }

    /**
     * Recursively flattens all string values contained in the given properties into a single stream.
     *
     * <p>{@link String} values are emitted directly, nested {@link Properties} values are flattened
     * recursively, and values of any other type are skipped.
     *
     * @param properties the properties whose string values should be flattened
     * @return a stream of every string value reachable from {@code properties}, including those held
     *         in nested {@link Properties}
     */
    private Stream<String> flattenValues(Properties properties) {
        return properties
                .values()
                .stream()
                .flatMap(v -> {
                    if (v instanceof String) {
                        return Stream.of((String) v);
                    } else if (v instanceof Properties) {
                        return flattenValues((Properties) v);
                    } else {
                        return Stream.empty();
                    }
                });
    }

    /**
     * Mutable accumulator that detects values appearing more than once while streaming.
     *
     * <p>It implements {@link Consumer} so that it can act as both the accumulator and the combiner
     * of a three-argument {@code collect} mutable reduction: {@link #accept(String)} records each
     * value and flags repeats, while {@link #combine(DuplicateFinder)} merges the state produced by a
     * second finder during parallel processing.
     */
    private static class DuplicateFinder implements Consumer<String> {
        /** Distinct values observed so far. */
        private final Set<String> processedValues = new HashSet<>();
        /** Values that have been observed more than once. */
        private final List<String> duplicates = new ArrayList<>();

        /**
         * Records a single value, flagging it as a duplicate if it has been seen before.
         *
         * <p>If the value is already present in {@link #processedValues} it is added to
         * {@link #duplicates}; otherwise it is added to {@link #processedValues}.
         *
         * @param o the value to record
         */
        @Override
        public void accept(String o) {
            if (processedValues.contains(o)) {
                duplicates.add(o);
            } else {
                processedValues.add(o);
            }
        }

        /**
         * Merges the state of another {@link DuplicateFinder} into this one.
         *
         * <p>Each value already processed by {@code other} is checked against this finder's
         * {@link #processedValues}: values seen in both are recorded as duplicates, while previously
         * unseen values are added to {@link #processedValues}. This serves as the combiner step of a
         * mutable reduction.
         *
         * @param other the finder whose accumulated state should be merged into this one
         */
        void combine(DuplicateFinder other) {
            other.processedValues
                    .forEach(v -> {
                        if (processedValues.contains(v)) {
                            duplicates.add(v);
                        } else {
                            processedValues.add(v);
                        }
                    });
        }
    }
}
