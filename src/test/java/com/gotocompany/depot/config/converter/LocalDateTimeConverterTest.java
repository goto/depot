package com.gotocompany.depot.config.converter;

import org.junit.Test;

import java.time.DateTimeException;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link LocalDateTimeConverter}, the Owner converter that parses an ISO-8601 string
 * into a {@link LocalDateTime}.
 *
 * <p>The tests share a single converter instance and assert both successful parsing of a valid
 * timestamp and the {@link DateTimeException} raised for malformed input.
 */
public class LocalDateTimeConverterTest {

    /**
     * Converter under test, shared across all scenarios.
     */
    private final LocalDateTimeConverter localDateTimeConverter = new LocalDateTimeConverter();

    /**
     * Verifies that {@link LocalDateTimeConverter#convert} parses a valid ISO-8601 timestamp.
     *
     * <p>Given the input {@code "2024-01-01T00:00:00"}, when {@code convert} is invoked, then the
     * result equals the {@link LocalDateTime} for midnight at the start of 1 January 2024.
     */
    @Test
    public void shouldConvertToLocalDateTime() {
        String input = "2024-01-01T00:00:00";
        LocalDateTime expected = LocalDateTime.of(2024, 1, 1, 0, 0, 0);

        LocalDateTime localDateTime = localDateTimeConverter.convert(null, input);

        assertEquals(expected, localDateTime);
    }

    /**
     * Verifies that {@link LocalDateTimeConverter#convert} rejects a malformed timestamp.
     *
     * <p>Given the unparseable input {@code "12-312024-01-01T00:00:00Z"}, when {@code convert} is
     * invoked, then a {@link DateTimeException} is thrown.
     */
    @Test(expected = DateTimeException.class)
    public void shouldThrowExceptionWhenInputIsInvalid() {
        String input = "12-312024-01-01T00:00:00Z";
        localDateTimeConverter.convert(null, input);
    }

}
