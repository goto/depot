package com.gotocompany.depot.config.converter;

import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

/**
 * Owner {@link Converter} that resolves a configuration property value into a
 * {@link SinkConnectorSchemaMessageMode} constant.
 *
 * <p>This converter is registered against the sink connector schema configuration through the
 * Aeon Owner {@code @ConverterClass} mechanism. It selects whether Depot interprets the log key
 * or the log message portion of an incoming record, exposing the choice as a strongly typed enum
 * rather than as a raw {@link String}.
 *
 * <p>The accepted tokens correspond to the names of the {@link SinkConnectorSchemaMessageMode}
 * constants ({@code LOG_KEY} and {@code LOG_MESSAGE}) and are matched case-insensitively.
 *
 * @see SinkConnectorSchemaMessageMode
 * @see Converter
 */
public class SinkConnectorSchemaMessageModeConverter implements Converter<SinkConnectorSchemaMessageMode> {
    /**
     * Converts the raw configuration value into the matching {@link SinkConnectorSchemaMessageMode} constant.
     *
     * <p>The supplied value is upper-cased using the platform default locale and then resolved
     * through {@link SinkConnectorSchemaMessageMode#valueOf(String)}, which makes the lookup
     * effectively case-insensitive while still requiring an otherwise exact match of a declared
     * constant name.
     *
     * @param method the Owner configuration accessor method whose property is being resolved;
     *               not inspected by this implementation
     * @param input  the raw property value to convert, expected to name a
     *               {@link SinkConnectorSchemaMessageMode} constant
     * @return the {@link SinkConnectorSchemaMessageMode} constant whose name matches {@code input}
     *         ignoring case
     * @throws IllegalArgumentException if {@code input} does not match any declared constant
     * @throws NullPointerException     if {@code input} is {@code null}
     */
    @Override
    public SinkConnectorSchemaMessageMode convert(Method method, String input) {
        return SinkConnectorSchemaMessageMode.valueOf(input.toUpperCase());
    }
}
