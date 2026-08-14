package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.Tuple;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Owner {@link Converter} that parses a delimited list of {@code key=value} pairs into a
 * {@code Map<String, String>} of labels.
 *
 * <p>Parsing and sanitisation are delegated to {@link ConverterUtils#convertToList(String)}, which
 * splits the input on commas, splits each entry on {@code "="}, trims the key and value, skips
 * malformed or empty-keyed entries, and truncates over-long values. The resulting tuples are then
 * collected into a map. This is typically used to attach user-defined labels, for example to
 * BigQuery or MaxCompute resources.
 *
 * @see ConverterUtils#convertToList(String)
 * @see Converter
 */
public class LabelMapConverter implements Converter<Map<String, String>> {

    /**
     * Converts the delimited {@code key=value} configuration string into a label map.
     *
     * <p>The input is first transformed into a list of key-value tuples by
     * {@link ConverterUtils#convertToList(String)} and then collected into a map keyed by the first
     * tuple element and valued by the second.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, a comma-separated list of {@code key=value} label pairs
     * @return a map of label keys to label values; empty when no valid entries are present
     * @throws IllegalStateException if two entries resolve to the same key
     * @throws NullPointerException  if {@code input} is {@code null}
     */
    public Map<String, String> convert(Method method, String input) {
        List<Tuple<String, String>> listResult = ConverterUtils.convertToList(input);
        return listResult.stream().collect(Collectors.toMap(Tuple::getFirst, Tuple::getSecond));
    }
}

