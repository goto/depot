package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.http.enums.HttpRequestType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link HttpRequestType} constant.
 *
 * <p>This converter is registered against the HTTP sink configuration through the Aeon Owner
 * {@code @ConverterClass} mechanism. It selects whether the sink emits one HTTP request per record
 * or batches multiple records into a single request, exposing the choice to Depot as a strongly
 * typed enum rather than as a raw {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link HttpRequestType} constants
 * ({@code SINGLE} and {@code BATCH}) and are matched case-insensitively.
 *
 * @see HttpRequestType
 * @see Converter
 */
public class HttpRequestTypeConverter implements Converter<HttpRequestType> {

    /**
     * Converts the raw configuration value into the matching {@link HttpRequestType} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link HttpRequestType#valueOf(String)}, which makes the lookup effectively
     * case-insensitive while still requiring an otherwise exact match of a declared constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link HttpRequestType} constant
     * @return the {@link HttpRequestType} constant whose name matches {@code input} ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public HttpRequestType convert(Method method, String input) {
        return HttpRequestType.valueOf(input.toUpperCase());
    }
}
