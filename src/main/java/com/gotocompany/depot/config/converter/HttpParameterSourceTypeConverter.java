package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpParameterSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link HttpParameterSourceType} constant.
 *
 * <p>This converter is registered against HTTP sink configuration accessors through the
 * Aeon Owner {@code @ConverterClass} mechanism so that string properties describing where the
 * data backing an HTTP request parameter should be read from are surfaced to the rest of Depot
 * as a strongly typed enum rather than as a raw {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link HttpParameterSourceType}
 * constants ({@code KEY} and {@code MESSAGE}) and are matched case-insensitively.
 *
 * @see HttpParameterSourceType
 * @see Converter
 */
public class HttpParameterSourceTypeConverter implements Converter<HttpParameterSourceType> {
    /**
     * Converts the raw configuration value into the matching {@link HttpParameterSourceType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link HttpParameterSourceType#valueOf(String)}, which makes the lookup effectively
     * case-insensitive while still requiring an otherwise exact match of a declared constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link HttpParameterSourceType} constant
     * @return the {@link HttpParameterSourceType} constant whose name matches {@code input}
     *         ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public HttpParameterSourceType convert(Method method, String input) {
        return HttpParameterSourceType.valueOf(input.toUpperCase());
    }
}
