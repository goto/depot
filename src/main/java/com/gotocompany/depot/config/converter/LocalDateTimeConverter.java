package com.gotocompany.depot.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Owner {@link Converter} that parses a configuration property value into a {@link LocalDateTime}.
 *
 * <p>The value is interpreted using {@link DateTimeFormatter#ISO_DATE_TIME}, that is the ISO-8601
 * date-time format such as {@code 2011-12-03T10:15:30}, optionally including an offset or zone. This
 * is used by configuration that needs a wall-clock timestamp expressed as a strongly typed value
 * rather than as a raw {@link String}.
 *
 * @see Converter
 * @see DateTimeFormatter#ISO_DATE_TIME
 */
public class LocalDateTimeConverter implements Converter<LocalDateTime> {

    /**
     * Parses the configuration value into a {@link LocalDateTime} using the ISO-8601 date-time format.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param s      the raw property value, expected to be an ISO-8601 local date-time string
     * @return the {@link LocalDateTime} parsed from {@code s}
     * @throws java.time.format.DateTimeParseException if {@code s} cannot be parsed as an ISO-8601
     *         local date-time
     * @throws NullPointerException                    if {@code s} is {@code null}
     */
    @Override
    public LocalDateTime convert(Method method, String s) {
        return LocalDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME);
    }

}
