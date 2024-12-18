package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.exception.ConfigurationException;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class ZoneIdConverterTest {

    @Test
    public void shouldParseValidZoneId() {
        String zoneId = "UTC";
        ZoneIdConverter zoneIdConverter = new ZoneIdConverter();

        ZoneId result = zoneIdConverter.convert(null, zoneId);

        assertEquals(ZoneId.of(zoneId), result);
    }

    @Test(expected = ConfigurationException.class)
    public void shouldThrowDateTimeExceptionGivenInvalidZoneId() {
        String zoneId = "InvalidZoneId";
        ZoneIdConverter zoneIdConverter = new ZoneIdConverter();

        zoneIdConverter.convert(null, zoneId);
    }

}
