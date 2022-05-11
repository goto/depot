package io.odpf.depot.config.converter;

import io.odpf.depot.message.InputSchemaMessageMode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;


public class InputSchemaMessageModeConverterTest {

    @Test
    public void shouldConvertLogKey() {
        InputSchemaMessageModeConverter converter = new InputSchemaMessageModeConverter();
        InputSchemaMessageMode mode = converter.convert(null, "LOG_KEY");
        Assert.assertEquals(InputSchemaMessageMode.LOG_KEY, mode);
    }


    @Test
    public void shouldThrowException() {
        InputSchemaMessageModeConverter converter = new InputSchemaMessageModeConverter();
        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            converter.convert(null, "Invalid");
        });
        Assert.assertEquals("No enum constant io.odpf.depot.message.InputSchemaMessageMode.INVALID", exception.getMessage());
    }
}