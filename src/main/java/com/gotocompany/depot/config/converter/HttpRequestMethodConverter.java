package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link HttpRequestMethodType} constant.
 *
 * <p>This converter is registered against the HTTP sink configuration through the Aeon Owner
 * {@code @ConverterClass} mechanism. It selects the HTTP verb the sink uses when dispatching
 * requests, exposing the choice to Depot as a strongly typed enum rather than as a raw
 * {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link HttpRequestMethodType} constants
 * ({@code PUT}, {@code POST}, {@code PATCH} and {@code DELETE}) and are matched case-insensitively.
 *
 * @see HttpRequestMethodType
 * @see Converter
 */
public class HttpRequestMethodConverter implements Converter<HttpRequestMethodType> {

    /**
     * Converts the raw configuration value into the matching {@link HttpRequestMethodType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link HttpRequestMethodType#valueOf(String)}, which makes the lookup effectively
     * case-insensitive while still requiring an otherwise exact match of a declared constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link HttpRequestMethodType} constant
     * @return the {@link HttpRequestMethodType} constant whose name matches {@code input}
     *         ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public HttpRequestMethodType convert(Method method, String input) {
        return HttpRequestMethodType.valueOf(input.toUpperCase());
    }
}
