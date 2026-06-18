package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


/**
 * Unit tests for {@link SinkConnectorSchemaMessageModeConverter}, the Owner converter that maps a
 * configuration string to a {@link SinkConnectorSchemaMessageMode} enum constant.
 *
 * <p>Each test instantiates a fresh converter and invokes
 * {@link SinkConnectorSchemaMessageModeConverter#convert} with a {@code null} accessor method,
 * asserting either the resolved enum constant or, for an unknown value, the {@link RuntimeException}
 * raised by the underlying enum lookup.
 */
public class SinkConnectorSchemaMessageModeConverterTest {

    /**
     * Verifies that {@link SinkConnectorSchemaMessageModeConverter#convert} resolves the
     * {@code "LOG_KEY"} value.
     *
     * <p>Given the input {@code "LOG_KEY"}, when {@code convert} is invoked, then the result is
     * {@link SinkConnectorSchemaMessageMode#LOG_KEY}.
     */
    @Test
    public void shouldConvertLogKey() {
        SinkConnectorSchemaMessageModeConverter converter = new SinkConnectorSchemaMessageModeConverter();
        SinkConnectorSchemaMessageMode mode = converter.convert(null, "LOG_KEY");
        Assert.assertEquals(SinkConnectorSchemaMessageMode.LOG_KEY, mode);
    }


    /**
     * Verifies that {@link SinkConnectorSchemaMessageModeConverter#convert} rejects an unknown
     * value.
     *
     * <p>Given the input {@code "Invalid"}, when {@code convert} is invoked, then a
     * {@link RuntimeException} is thrown whose message reports the missing enum constant
     * {@code SinkConnectorSchemaMessageMode.INVALID}.
     */
    @Test
    public void shouldThrowException() {
        SinkConnectorSchemaMessageModeConverter converter = new SinkConnectorSchemaMessageModeConverter();
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            converter.convert(null, "Invalid");
        });
        Assert.assertEquals("No enum constant com.gotocompany.depot.message.SinkConnectorSchemaMessageMode.INVALID", exception.getMessage());
    }
}
