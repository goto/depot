package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that normalises an empty configuration value to {@code null}.
 *
 * <p>Non-empty values are returned unchanged, while an empty string is converted to {@code null}.
 * This is convenient for optional properties where the absence of a meaningful value should be
 * represented as {@code null} rather than as an empty {@link String}.
 *
 * @see Converter
 */
public class EmptyStringToNull implements Converter<String> {
    /**
     * Returns the input unchanged, or {@code null} when the input is an empty string.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to normalise
     * @return {@code null} if {@code input} is empty, otherwise {@code input} unchanged
     * @throws NullPointerException if {@code input} is {@code null}
     */
    @Override
    public String convert(Method method, String input) {
        if (input.isEmpty()) {
            return null;
        }
        return input;
    }
}
