package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.common.TupleString;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that parses a single {@code key=value} configuration entry into a
 * {@link TupleString}.
 *
 * <p>The converter splits the input on the {@link #VALUE_SEPARATOR} and returns the first two
 * segments as the {@code first} (key) and {@code second} (value) of a {@link TupleString}. The
 * {@link #ELEMENT_SEPARATOR} constant documents the delimiter used to separate multiple entries in
 * the broader configuration grammar and is published for collaborators that need it.
 *
 * <p>An empty input yields {@code null}.
 *
 * @see TupleString
 * @see Converter
 */
public class ConfToListConverter implements Converter<TupleString> {
    /** Delimiter that separates consecutive entries within a configuration value. */
    public static final String ELEMENT_SEPARATOR = ",";
    /** Delimiter that separates a key from its value within a single entry. */
    public static final String VALUE_SEPARATOR = "=";

    /**
     * Parses a single {@code key=value} entry into a {@link TupleString}.
     *
     * <p>If the input is empty {@code null} is returned. Otherwise the input is split on the
     * {@link #VALUE_SEPARATOR} and a {@link TupleString} is constructed from the first segment (the
     * key) and the second segment (the value).
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value, expected to be a single {@code key=value} pair
     * @return a {@link TupleString} holding the parsed key and value, or {@code null} if
     *         {@code input} is empty
     * @throws ArrayIndexOutOfBoundsException if {@code input} does not contain the
     *         {@link #VALUE_SEPARATOR} and therefore has no value segment
     * @throws NullPointerException if {@code input} is {@code null}
     */
    @Override
    public TupleString convert(Method method, String input) {
        if (input.isEmpty()) {
            return null;
        }
        String[] split = input.split(VALUE_SEPARATOR);
        return new TupleString(split[0], split[1]);
    }
}
