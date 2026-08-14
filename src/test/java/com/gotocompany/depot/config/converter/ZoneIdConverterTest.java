package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ZoneIdConverter}, the Owner converter that parses a configuration string
 * into a {@link ZoneId}.
 *
 * <p>Each test instantiates a fresh converter and invokes {@link ZoneIdConverter#convert} with a
 * {@code null} accessor method, asserting either the parsed {@link ZoneId} or the
 * {@link ConfigurationException} raised for an invalid zone identifier.
 */
public class ZoneIdConverterTest {

    /**
     * Verifies that {@link ZoneIdConverter#convert} parses a valid zone identifier.
     *
     * <p>Given the input {@code "UTC"}, when {@code convert} is invoked, then the result equals
     * {@link ZoneId#of(String)} for {@code "UTC"}.
     */
    @Test
    public void shouldParseValidZoneId() {
        String zoneId = "UTC";
        ZoneIdConverter zoneIdConverter = new ZoneIdConverter();

        ZoneId result = zoneIdConverter.convert(null, zoneId);

        assertEquals(ZoneId.of(zoneId), result);
    }

    /**
     * Verifies that {@link ZoneIdConverter#convert} rejects an invalid zone identifier.
     *
     * <p>Given the input {@code "InvalidZoneId"}, when {@code convert} is invoked, then a
     * {@link ConfigurationException} is thrown, wrapping the underlying date-time parse failure.
     */
    @Test(expected = ConfigurationException.class)
    public void shouldThrowDateTimeExceptionGivenInvalidZoneId() {
        String zoneId = "InvalidZoneId";
        ZoneIdConverter zoneIdConverter = new ZoneIdConverter();

        zoneIdConverter.convert(null, zoneId);
    }

}
