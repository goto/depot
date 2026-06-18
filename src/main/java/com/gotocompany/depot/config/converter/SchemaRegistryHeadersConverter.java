package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;
import org.aeonbits.owner.Tokenizer;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;

import java.lang.reflect.Method;
import java.util.regex.Pattern;

/**
 * Owner {@link Converter} and {@link Tokenizer} that turns a configuration value into Apache
 * HttpComponents {@link Header} instances for talking to a schema registry.
 *
 * <p>As a {@link Tokenizer}, {@link #tokens(String)} splits a single property containing several
 * comma-separated {@code name:value} headers into individual header tokens; Owner then invokes
 * {@link #convert(Method, String)} on each token to build a {@link BasicHeader}. Implementing both
 * interfaces allows a single multi-valued property to be exposed as an array or collection of
 * {@link Header} values.
 *
 * @see Converter
 * @see Tokenizer
 */
public class SchemaRegistryHeadersConverter implements Converter<Header>, Tokenizer {

    /**
     * Converts a single {@code name:value} token into an Apache HttpComponents {@link Header}.
     *
     * <p>The token is split on the {@code ":"} delimiter; the first segment becomes the header name
     * and the second segment becomes the header value, both trimmed, and a {@link BasicHeader} is
     * created from them.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  a single header token in {@code name:value} form
     * @return a {@link Header} carrying the trimmed name and value
     * @throws ArrayIndexOutOfBoundsException if {@code input} does not contain the {@code ":"} delimiter
     * @throws NullPointerException           if {@code input} is {@code null}
     */
    @Override
    public Header convert(Method method, String input) {
        String[] split = input.split(":");
        return new BasicHeader(split[0].trim(), split[1].trim());
    }

    /**
     * Splits a multi-header configuration value into individual {@code name:value} tokens.
     *
     * <p>The value is split on commas; each candidate is trimmed and retained only if it splits into
     * exactly two non-empty parts around a {@code ":"} (a header name and a header value). The
     * surviving tokens are returned for Owner to convert individually. If no candidate is valid an
     * {@link IllegalArgumentException} is raised.
     *
     * @param values the raw property value containing zero or more comma-separated {@code name:value}
     *               headers
     * @return an array of the valid, trimmed header tokens
     * @throws IllegalArgumentException if {@code values} contains no valid {@code name:value} header
     * @throws NullPointerException     if {@code values} is {@code null}
     */
    @Override
    public String[] tokens(String values) {
        String[] headers = Pattern.compile(",").splitAsStream(values).map(String::trim)
                .filter(s -> {
                    String[] args = s.split(":");
                    return args.length == 2 && args[0].trim().length() > 0 && args[1].trim().length() > 0;
                }).toArray(String[]::new);
        if (headers.length == 0) {
            throw new IllegalArgumentException(String.format("provided headers %s is not valid", values));
        }

        return headers;
    }

}
